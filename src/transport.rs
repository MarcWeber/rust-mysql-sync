//! Client transport abstraction: encrypted QUIC (default) or plaintext TCP.
//!
//! Each table task uses one logical bidirectional stream:
//!   - QUIC: a bidi stream multiplexed over the shared connection.
//!   - TCP:  a dedicated TCP connection (plaintext, explicit opt-in).

use crate::config::{Config, ALPN};
use crate::proto::{self, Framing, Request, Response};
use crate::tls;
use anyhow::{bail, Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

pub type SendHalf = Box<dyn AsyncWrite + Unpin + Send>;
pub type RecvHalf = Box<dyn AsyncRead + Unpin + Send>;

enum Kind {
    Quic {
        conn: quinn::Connection,
        endpoint: quinn::Endpoint,
        hello_done: AtomicBool,
    },
    Tcp {
        addr: std::net::SocketAddr,
        timeout: Duration,
    },
}

pub struct Session {
    kind: Kind,
}

/// QUIC keep-alive so long-running table comparisons do not hit the peer's
/// default 30 s idle timeout while both sides are busy in MySQL.
pub fn quic_transport() -> Result<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.max_idle_timeout(Some(
        Duration::from_secs(300)
            .try_into()
            .map_err(|_| anyhow::anyhow!("QUIC-Idle-Timeout ungültig"))?,
    ));
    Ok(transport)
}

impl Session {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        if cfg.plaintext_tcp() {
            eprintln!("WARNING: PLAINTEXT transport=tcp — only use inside a trusted tunnel");
            let addr = cfg.server_addr().await?;
            return Ok(Self {
                kind: Kind::Tcp {
                    addr,
                    timeout: Duration::from_secs(cfg.connect_timeout_secs.max(1)),
                },
            });
        }

        let server_addr = cfg.server_addr().await?;
        let verifier = tls::PinVerifier::new(cfg)?;
        let provider = tls::ring_provider();
        let mut client_crypto = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        client_crypto.alpn_protocols = vec![ALPN.to_vec()];
        let mut quinn_client = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
                .context("QUIC-TLS-Konfiguration")?,
        ));
        quinn_client.transport_config(Arc::new(quic_transport()?));
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(quinn_client);

        log::info!("Verbinde mit {server_addr} …");
        let connect = endpoint.connect(server_addr, "mysql-sync")?;
        let conn = tokio::time::timeout(
            Duration::from_secs(cfg.connect_timeout_secs.max(1)),
            connect,
        )
        .await
        .map_err(|_| anyhow::anyhow!("Connect-Timeout nach {}s", cfg.connect_timeout_secs))?
        .context("QUIC-Handshake fehlgeschlagen")?;
        log::info!("Verbindung hergestellt ({})", conn.remote_address());
        Ok(Self {
            kind: Kind::Quic {
                conn,
                endpoint,
                hello_done: AtomicBool::new(false),
            },
        })
    }

    pub async fn open(&self, cfg: &Config) -> Result<(SendHalf, RecvHalf)> {
        match &self.kind {
            Kind::Quic {
                conn, hello_done, ..
            } => {
                let (mut send, mut recv) = conn.open_bi().await.context("QUIC-Stream öffnen")?;
                if !hello_done.swap(true, Ordering::SeqCst) {
                    hello(&mut send, &mut recv, cfg).await?;
                }
                Ok((Box::new(send), Box::new(recv)))
            }
            Kind::Tcp { addr, timeout } => {
                let stream = tokio::time::timeout(*timeout, TcpStream::connect(addr))
                    .await
                    .map_err(|_| anyhow::anyhow!("Connect-Timeout nach {}s", timeout.as_secs()))?
                    .with_context(|| format!("TCP-Verbindung zu {addr}"))?;
                stream.set_nodelay(true)?;
                let (mut recv, mut send) = stream.into_split();
                hello(&mut send, &mut recv, cfg).await?;
                Ok((Box::new(send), Box::new(recv)))
            }
        }
    }

    pub fn close(&self) {
        if let Kind::Quic { endpoint, .. } = &self.kind {
            endpoint.close(0u32.into(), b"done");
        }
    }
}

async fn hello<W, R>(send: &mut W, recv: &mut R, cfg: &Config) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
    R: AsyncRead + Unpin + ?Sized,
{
    let f = Framing::from_cfg(cfg);
    proto::write_request(
        send,
        &Request::Hello {
            version: proto::version(),
            token: cfg.auth_token.clone(),
            hash_algo: Some(cfg.hash_algo.to_string()),
        },
        &f,
    )
    .await?;
    match proto::read_response(recv, &f).await? {
        Some(Response::Ok { .. }) => {
            log::debug!("Server-Handshake ok");
            Ok(())
        }
        Some(Response::Err { message }) => bail!("Server lehnt ab: {message}"),
        Some(_) => bail!("Unerwartete Handshake-Antwort"),
        None => bail!("Keine Handshake-Antwort"),
    }
}
