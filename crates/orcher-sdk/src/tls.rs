//! How a client's or worker's TLS settings reach sdk-core, which makes every
//! connection: the client's, the worker's drivers' and the actor calls'.

use crate::client::ClientTlsConfig;

/// Whether `url` uses the `https` scheme.
pub(crate) fn is_https(url: &str) -> bool {
    url.trim()
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

/// The same settings in the form sdk-core takes.
pub(crate) fn to_core(tls: &ClientTlsConfig) -> orcher_sdk_core::TlsConfig {
    let mut core = orcher_sdk_core::TlsConfig::new();
    if let Some(ref ca) = tls.ca_cert {
        core = core.with_ca_cert(ca.clone());
    }
    if let (Some(ref cert), Some(ref key)) = (&tls.client_cert, &tls.client_key) {
        core = core.with_client_identity(cert.clone(), key.clone());
    }
    if let Some(ref domain) = tls.domain_name {
        core = core.with_domain_name(domain.clone());
    }
    core
}

/// Checks settings that would otherwise be dropped without a word.
///
/// Returns a reason when they are unusable for `url`; logs a warning for TLS
/// settings on an `http://` address, which is dialled in plaintext.
pub(crate) fn validate(url: &str, tls: Option<&ClientTlsConfig>) -> Result<(), String> {
    let Some(tls) = tls else {
        return Ok(());
    };
    if tls.client_cert.is_some() != tls.client_key.is_some() {
        return Err(
            "TLS client certificate and key must be set together (with_client_identity)"
                .to_string(),
        );
    }
    if !is_https(url) {
        tracing::warn!(
            server_url = %url,
            "TLS settings are ignored for a non-https:// address; the connection is plaintext. Use https:// to enable TLS"
        );
    }
    Ok(())
}

/// Checks that `url` and `tls` make a connection sdk-core can use: a valid
/// address, and TLS settings it accepts. Nothing is dialled.
pub(crate) fn check(url: &str, tls: Option<&ClientTlsConfig>) -> Result<(), String> {
    validate(url, tls)?;
    let mut options = orcher_sdk_core::ConnectOptions::new(url);
    if let Some(tls) = tls {
        options = options.with_tls(to_core(tls));
    }
    orcher_sdk_core::Connection::lazy(options)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn to_core_carries_every_setting() {
        let tls = ClientTlsConfig::new()
            .with_ca_cert(b"ca".to_vec())
            .with_client_identity(b"cert".to_vec(), b"key".to_vec())
            .with_domain_name("orcher.internal");
        let core = to_core(&tls);
        assert_eq!(core.ca_cert.as_deref(), Some(&b"ca"[..]));
        assert_eq!(core.client_cert.as_deref(), Some(&b"cert"[..]));
        assert_eq!(core.client_key.as_deref(), Some(&b"key"[..]));
        assert_eq!(core.domain_name.as_deref(), Some("orcher.internal"));
    }

    #[test]
    fn a_certificate_without_a_key_is_rejected() {
        let mut tls = ClientTlsConfig::new();
        tls.client_cert = Some(b"cert".to_vec());
        let err = check("https://orcher.example", Some(&tls)).unwrap_err();
        assert!(err.contains("must be set together"), "{err}");
    }

    #[tokio::test]
    async fn an_invalid_address_is_rejected() {
        assert!(check("not a url", None).is_err());
        assert!(check("https://orcher.example", None).is_ok());
    }

    /// Makes one call through a [`crate::Client`] on `scheme` to a bare TCP
    /// listener and returns the first bytes it sent, which tell a TLS
    /// ClientHello from an HTTP/2 preface.
    async fn first_bytes_sent(scheme: &str) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16];
            let n = socket.read(&mut buf).await.unwrap();
            buf.truncate(n);
            buf
        });
        let client = crate::Client::connect(format!("{scheme}://localhost:{port}"))
            .await
            .expect("the client builds");
        // The listener hangs up after reading, so the call itself fails; only
        // what reached the wire matters.
        let _ = tokio::time::timeout(Duration::from_secs(5), client.list_namespaces()).await;
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the client wrote something")
            .unwrap()
    }

    #[tokio::test]
    async fn an_https_client_speaks_tls_without_explicit_settings() {
        let sent = first_bytes_sent("https").await;
        // A TLS record starts with content type 22 (handshake).
        assert_eq!(sent.first(), Some(&0x16), "sent {sent:?}");
    }

    #[tokio::test]
    async fn an_http_client_stays_plaintext() {
        let sent = first_bytes_sent("http").await;
        assert!(sent.starts_with(b"PRI * HTTP/2.0"), "sent {sent:?}");
    }
}
