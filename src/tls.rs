//! TLS: Self-signed-Zertifikate (Server) + Fingerprint-Pinning (Client).
//!
//! Server: liest Zertifikat und Key als PEM aus der JSON-Config
//! (`tls_cert_pem` / `tls_key_pem`, erzeugt von `--generate-configs`).
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
    let t = s
        .trim()
        .trim_start_matches("sha256:")
        .trim_start_matches("SHA256:");
    let bytes = hex::decode(t).context("Fingerprint ist kein gültiges Hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Fingerprint muss 32 Bytes (64 Hex-Zeichen) haben"))
}

/// Erzeugt ein neues Self-signed-Zertifikat: (cert_pem, key_pem, fingerprint).
pub fn generate_identity() -> Result<(String, String, String)> {
    let mut params =
        rcgen::CertificateParams::new(vec!["mysql-sync".to_string()]).context("rcgen-Params")?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "mysql-sync");
    let key_pair = rcgen::KeyPair::generate().context("Key-Erzeugung fehlgeschlagen")?;
    let cert = params
        .self_signed(&key_pair)
        .context("Self-Signing fehlgeschlagen")?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let (certs, _) = parse_pem(cert_pem.as_bytes(), key_pem.as_bytes())?;
    let fp = fingerprint_of(certs[0].as_ref());
    Ok((cert_pem, key_pem, fp))
}

/// Liest die Server-Identität ausschließlich aus den PEM-Feldern der Config.
pub fn server_identity(
    cfg: &Config,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>, String)> {
    let (cp, kp) = match (&cfg.tls_cert_pem, &cfg.tls_key_pem) {
        (Some(cp), Some(kp)) => (cp, kp),
        (None, None) => bail!(
            "Server-Config braucht \"tls_cert_pem\" und \"tls_key_pem\" \
             (erzeuge sie mit --generate-configs)"
        ),
        _ => bail!("tls_cert_pem und tls_key_pem müssen gemeinsam gesetzt sein"),
    };
    let (certs, key) = parse_pem(cp.as_bytes(), kp.as_bytes())?;
    let fp = fingerprint_of(certs[0].as_ref());
    Ok((certs, key, fp))
}

fn parse_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut cr = cert_pem;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cr)
        .collect::<std::result::Result<_, _>>()
        .context("Zertifikat-PEM ungültig")?;
    if certs.is_empty() {
        bail!("Kein Zertifikat im PEM");
    }
    let mut kr = key_pem;
    let key = rustls_pemfile::private_key(&mut kr)
        .context("Key-PEM ungültig")?
        .context("Kein privater Key im PEM")?;
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
                log::warn!(
                    "→ Für Produktion in config-client.json unter \"server_fingerprint\" pinnen!"
                );
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
