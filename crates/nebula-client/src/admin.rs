use std::{error::Error, io, path::Path};

use reqwest::{header, Method, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

pub(super) struct AdminClient {
    base_url: Url,
    client: reqwest::Client,
    cookie: Option<String>,
    csrf_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct User {
    pub username: String,
    pub role: String,
    pub enabled: bool,
    pub created_at: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct Token {
    pub id: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct CreatedToken {
    pub id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct BootstrapResult {
    pub user: User,
    pub token: CreatedToken,
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    token: &'a str,
}

#[derive(Deserialize)]
struct LoginResponse {
    csrf_token: String,
}

impl AdminClient {
    pub(super) fn new(
        address: &str,
        tls_ca: Option<impl AsRef<Path>>,
    ) -> Result<Self, Box<dyn Error>> {
        let mut base_url = Url::parse(address)?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.host().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "admin address must be an HTTP or HTTPS URL",
            )
            .into());
        }
        base_url.set_query(None);
        base_url.set_fragment(None);
        let mut builder = reqwest::Client::builder();
        if let Some(path) = tls_ca {
            let content = std::fs::read(path)?;
            let certificates =
                rustls_pemfile::certs(&mut content.as_slice()).collect::<Result<Vec<_>, _>>()?;
            if certificates.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "TLS CA file contains no certificates",
                )
                .into());
            }
            for certificate in certificates {
                builder = builder
                    .add_root_certificate(reqwest::Certificate::from_der(certificate.as_ref())?);
            }
        }
        Ok(Self {
            base_url,
            client: builder.build()?,
            cookie: None,
            csrf_token: None,
        })
    }

    pub(super) async fn authenticate(&mut self, token: &str) -> Result<(), Box<dyn Error>> {
        self.require_https()?;
        let response = self
            .client
            .post(self.endpoint(&["api", "login"])?)
            .json(&LoginRequest { token })
            .send()
            .await?;
        let response = ensure_success(response).await?;
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::to_owned)
            .ok_or_else(|| io::Error::other("admin API login did not return a session cookie"))?;
        let body: LoginResponse = response.json().await?;
        self.cookie = Some(cookie);
        self.csrf_token = Some(body.csrf_token);
        Ok(())
    }

    pub(super) async fn logout(&self) -> Result<(), Box<dyn Error>> {
        self.request(Method::POST, &["api", "logout"], None).await?;
        Ok(())
    }

    pub(super) async fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<BootstrapResult, Box<dyn Error>> {
        self.require_https()?;
        self.json_request(
            Method::POST,
            &["api", "bootstrap-admin"],
            Some(json!({ "credential": credential, "username": username })),
        )
        .await
    }

    pub(super) async fn create_user(
        &self,
        username: &str,
        role: &str,
    ) -> Result<User, Box<dyn Error>> {
        self.json_request(
            Method::POST,
            &["api", "users"],
            Some(json!({ "username": username, "role": role })),
        )
        .await
    }

    pub(super) async fn list_users(&self) -> Result<Vec<User>, Box<dyn Error>> {
        self.json_request(Method::GET, &["api", "users"], None)
            .await
    }

    pub(super) async fn show_user(&self, username: &str) -> Result<User, Box<dyn Error>> {
        self.json_request(Method::GET, &["api", "users", username], None)
            .await
    }

    pub(super) async fn set_user_role(
        &self,
        username: &str,
        role: &str,
    ) -> Result<(), Box<dyn Error>> {
        self.request(
            Method::PUT,
            &["api", "users", username, "role"],
            Some(json!({ "role": role })),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn set_user_enabled(
        &self,
        username: &str,
        enabled: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.request(
            Method::PUT,
            &["api", "users", username, "enabled"],
            Some(json!({ "enabled": enabled })),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn delete_user(&self, username: &str) -> Result<(), Box<dyn Error>> {
        self.request(Method::DELETE, &["api", "users", username], None)
            .await?;
        Ok(())
    }

    pub(super) async fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, Box<dyn Error>> {
        self.json_request(
            Method::POST,
            &["api", "users", username, "tokens"],
            Some(json!({ "expires_at": expires_at })),
        )
        .await
    }

    pub(super) async fn list_tokens(&self, username: &str) -> Result<Vec<Token>, Box<dyn Error>> {
        self.json_request(Method::GET, &["api", "users", username, "tokens"], None)
            .await
    }

    pub(super) async fn revoke_token(&self, token_id: &str) -> Result<(), Box<dyn Error>> {
        self.request(Method::DELETE, &["api", "tokens", token_id], None)
            .await?;
        Ok(())
    }

    async fn json_request<T: DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
    ) -> Result<T, Box<dyn Error>> {
        let response = self.request(method, segments, body).await?;
        Ok(response.json().await?)
    }

    async fn request(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
    ) -> Result<Response, Box<dyn Error>> {
        let mut request = self.client.request(method, self.endpoint(segments)?);
        if let Some(cookie) = self.cookie.as_deref() {
            request = request.header(header::COOKIE, cookie);
        }
        if let Some(csrf_token) = self.csrf_token.as_deref() {
            request = request.header("x-nebula-csrf", csrf_token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        ensure_success(request.send().await?).await
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, Box<dyn Error>> {
        let mut endpoint = self.base_url.clone();
        {
            let mut path = endpoint.path_segments_mut().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid admin base URL")
            })?;
            path.pop_if_empty();
            path.extend(segments.iter().copied());
        }
        Ok(endpoint)
    }

    fn require_https(&self) -> Result<(), Box<dyn Error>> {
        if self.base_url.scheme() != "https" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTPS is required for administrative credentials",
            )
            .into());
        }
        Ok(())
    }
}

async fn ensure_success(response: Response) -> Result<Response, Box<dyn Error>> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let message = response.text().await.unwrap_or_default();
    Err(io::Error::other(format!("admin API returned {status}: {message}")).into())
}
