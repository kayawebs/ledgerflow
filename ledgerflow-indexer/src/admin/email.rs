use serde_json::json;

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone)]
pub struct PostmarkEmailSender {
    server_token: Option<String>,
    from: String,
    client: reqwest::Client,
}

impl PostmarkEmailSender {
    pub fn new(server_token: Option<String>, from: String) -> Self {
        Self {
            server_token,
            from,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send_verification_code(&self, to: &str, code: &str) -> LedgerResult<bool> {
        let Some(token) = self.server_token.as_ref() else {
            return Ok(false);
        };
        let response = self
            .client
            .post("https://api.postmarkapp.com/email")
            .header("X-Postmark-Server-Token", token)
            .json(&json!({
                "From": self.from,
                "To": to,
                "Subject": "Your LedgerFlow verification code",
                "TextBody": format!("Your LedgerFlow verification code is {code}. It expires in 10 minutes."),
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "postmark send failed: {status} {body}"
            )));
        }
        Ok(true)
    }
}
