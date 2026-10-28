//! TLS: Self-signed-Zertifikate (Server) + Fingerprint-Pinning (Client).
//!
//! Server: erzeugt beim ersten Start ein Self-signed-Zertifikat neben der
//! Config (mysql-sync-cert.pem / mysql-sync-key.pem) und schreibt den
//! SHA-256-Fingerprint in <config>.fingerprint.
//! Client: prüft via custom ServerCertVerifier, ob der Fingerprint in
//! "server_fingerprint" (sha256:<hex>) passt. Ohne Pinning: TOFU mit Warnung.

use crate::config::Config;
use anyhow::{bail, Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub fn fingerprint_of(der: &[u8]) -> String {
    let digest = Sha256::digest(der);
    format!("sha256:{}", hex::encode(digest))
}

fn parse_fingerprint(s: &str) -> Result<[u8; 32]> {
    let t = s.trim().trim_start_matches("sha256:").trim_start_matches("SHA256:");
    let bytes = hex::decode(t).context("Fingerprint ist kein gültiges Hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Fingerprint muss 32 Bytes (64 Hex-Zeichen) haben"))
}

/// Lädt oder erzeugt Server-Identität. Liefert (certs, key, fingerprint).
pub fn server_identity(cfg: &Config, config_path: &std::path::Path) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>, String)> {
    if let (Some(cp), Some(kp)) = (&cfg.tls_cert, &cfg.tls_key) {
        let (certs, key) = load_pem(std::path::Path::new(cp), std::path::Path::new(kp))?;
        let fp = fingerprint_of(certs[0].as_ref());
        return Ok((certs, key, fp));
    }

    let stem: String = config_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "config".into());
    let dir = config_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let cert_path = dir.join(format!("{stem}-cert.pem"));
    let key_path = dir.join(format!("{stem}-key.pem"));

    if cert_path.exists() && key_path.exists() {
        let (certs, key) = load_pem(&cert_path, &key_path)?;
        let fp = fingerprint_of(certs[0].as_ref());
        return Ok((certs, key, fp));
    }

    log::info!("Erzeuge neues Self-signed-Zertifikat …");
    let mut params = rcgen::CertificateParams::new(vec!["mysql-sync".to_string()])
        .context("rcgen-Params")?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "mysql-sync");
    let key_pair = rcgen::KeyPair::generate().context("Key-Erzeugung fehlgeschlagen")?;
    let cert = params
        .self_signed(&key_pair)
        .context("Self-Signing fehlgeschlagen")?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    std::fs::write(&cert_path, &cert_pem).with_context(|| format!("Schreiben {}", cert_path.display()))?;
    std::fs::write(&key_path, &key_pem).with_context(|| format!("Schreiben {}", key_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }

    let (certs, key) = load_pem(&cert_path, &key_path)?;
    let fp = fingerprint_of(certs[0].as_ref());
    let fp_path = config_path.with_extension("fingerprint");
    let _ = std::fs::write(&fp_path, format!("{fp}\n"));
    log::info!("Server-Fingerprint: {fp}");
    log::info!("→ in config-client.json unter \"server_fingerprint\" eintragen (Pinning)");
    Ok((certs, key, fp))
}

fn load_pem(cert_path: &std::path::Path, key_path: &std::path::Path) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_pem = std::fs::read(cert_path).with_context(|| format!("Lesen {}", cert_path.display()))?;
    let key_pem = std::fs::read(key_path).with_context(|| format!("Lesen {}", key_path.display()))?;
    let mut cr = &cert_pem[..];
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cr)
        .collect::<std::result::Result<_, _>>()
        .context("Zertifikat-PEM ungültig")?;
    if certs.is_empty() {
        bail!("Kein Zertifikat in {}", cert_path.display());
    }
    let mut kr = &key_pem[..];
    let key = rustls_pemfile::private_key(&mut kr)
        .context("Key-PEM ungültig")?
        .with_context(|| format!("Kein privater Key in {}", key_path.display()))?;
    Ok((certs, key))
}

/// Client-seitiger Verifier: pinnt Server-Zertifikat per SHA-256 oder TOFU.
#[derive(Debug)]
pub struct PinVerifier {
    pub pinned: Option<[u8; 32]>,
}

impl PinVerifier {
    pub fn new(cfg: &Config) -> Result<Self> {
        let pinned = match &cfg.server_fingerprint {
            Some(fp) => Some(parse_fingerprint(fp)?),
            None => None,
        };
        Ok(Self { pinned })
    }
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let fp = fingerprint_of(end_entity.as_ref());
        match &self.pinned {
            Some(pin) => {
                let digest: [u8; 32] = Sha256::digest(end_entity.as_ref()).into();
                if *pin == digest {
                    Ok(ServerCertVerified::assertion())
                } else {
                    log::error!(
                        "Server-Fingerprint stimmt NICHT überein!\n  erwartet: sha256:{}\n  erhalten: {fp}",
                        hex::encode(pin)
                    );
                    Err(rustls::Error::General(
                        "Server-Fingerprint stimmt nicht überein (MITM?)".into(),
                    ))
                }
            }
            None => {
                log::warn!("TOFU: Server-Fingerprint unbeprüft akzeptiert: {fp}");
                log::warn!("→ Für Produktion in config-client.json unter \"server_fingerprint\" pinnen!");
                Ok(ServerCertVerified::assertion())
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme as S;
        vec![
            S::ED25519,
            S::ECDSA_NISTP256_SHA256,
            S::RSA_PSS_SHA256,
            S::RSA_PKCS1_SHA256,
            S::RSA_PKCS1_SHA384,
            S::RSA_PKCS1_SHA512,
        ]
    }
}

pub fn ring_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}
