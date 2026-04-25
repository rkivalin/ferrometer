use std::path::Path;

use crate::config::TlsConfig;
use crate::error::{Error, Result};

/// Apply TLS settings from `cfg` to `builder` and return the updated
/// builder. Reads cert/key/CA files from disk; missing files surface as
/// FileRead errors. No-op when none of the fields are set.
pub async fn configure(
    mut builder: reqwest::ClientBuilder,
    cfg: &TlsConfig,
) -> Result<reqwest::ClientBuilder> {
    if let Some(ca_path) = &cfg.ca_cert_file {
        let pem = read_pem(ca_path).await?;
        for cert in reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| {
            Error::Config(format!(
                "ca-cert-file {}: parse failed: {e}",
                ca_path.display()
            ))
        })? {
            builder = builder.add_root_certificate(cert);
        }
    }

    if let Some(cert_path) = &cfg.client_cert_file {
        let cert_pem = read_pem(cert_path).await?;
        // The key may live in the same file (bundled) or in a separate
        // client-key-file. reqwest::Identity::from_pem expects both in
        // one buffer, so concatenate when split.
        let identity_pem = match &cfg.client_key_file {
            Some(key_path) if key_path != cert_path => {
                let key_pem = read_pem(key_path).await?;
                let mut combined = cert_pem;
                if !combined.ends_with(b"\n") {
                    combined.push(b'\n');
                }
                combined.extend_from_slice(&key_pem);
                combined
            }
            _ => cert_pem,
        };

        let identity = reqwest::Identity::from_pem(&identity_pem).map_err(|e| {
            Error::Config(format!(
                "client-cert-file {}: identity build failed (key missing or invalid?): {e}",
                cert_path.display()
            ))
        })?;
        builder = builder.identity(identity);
    }

    Ok(builder)
}

async fn read_pem(path: &Path) -> Result<Vec<u8>> {
    tokio::fs::read(path).await.map_err(|e| Error::FileRead {
        path: path.to_path_buf(),
        source: e,
    })
}
