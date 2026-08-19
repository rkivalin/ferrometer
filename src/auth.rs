use base64::Engine;

use crate::config::AuthConfig;
use crate::error::{Error, Result};

/// Build the `Authorization` header value for an HTTP client. Returns `Ok(None)`
/// when no auth is configured. Method exclusivity is already enforced by
/// `AuthConfig::validate()` at config load — this function just reads files
/// and assembles the header.
pub async fn resolve_header(cfg: &AuthConfig) -> Result<Option<String>> {
    if let Some(value) = &cfg.authorization {
        return Ok(Some(value.clone()));
    }

    if cfg.bearer_token.is_some() || cfg.bearer_token_file.is_some() {
        let token = match (&cfg.bearer_token, &cfg.bearer_token_file) {
            (Some(t), _) => t.clone(),
            (None, Some(p)) => read_trimmed(p).await?,
            (None, None) => unreachable!("guarded by outer if"),
        };
        return Ok(Some(format!("Bearer {token}")));
    }

    if let Some(username) = &cfg.username {
        let password = match (&cfg.password, &cfg.password_file) {
            (Some(p), _) => p.clone(),
            (None, Some(path)) => read_trimmed(path).await?,
            (None, None) => unreachable!("AuthConfig::validate guards this"),
        };
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        return Ok(Some(format!("Basic {encoded}")));
    }

    Ok(None)
}

async fn read_trimmed(path: &std::path::Path) -> Result<String> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| Error::FileRead {
            path: path.to_path_buf(),
            source: e,
        })?;
    Ok(content.trim().to_string())
}
