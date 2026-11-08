use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    ReadOnly,
    ReadWrite,
    Admin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "readonly",
            Self::ReadWrite => "readwrite",
            Self::Admin => "admin",
        }
    }

    pub fn can_read(self) -> bool {
        true
    }

    pub fn can_write(self) -> bool {
        matches!(self, Self::ReadWrite | Self::Admin)
    }

    pub fn is_admin(self) -> bool {
        self == Self::Admin
    }
}

impl TryFrom<&str> for Role {
    type Error = AuthError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "readonly" => Ok(Self::ReadOnly),
            "readwrite" => Ok(Self::ReadWrite),
            "admin" => Ok(Self::Admin),
            _ => Err(AuthError::InvalidRole),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct User {
    pub id: String,
    pub username: String,
    pub role: Role,
    pub enabled: bool,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedIdentity {
    pub user_id: String,
    pub username: String,
    pub role: Role,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenMetadata {
    pub id: String,
    pub user_id: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedToken {
    pub metadata: TokenMetadata,
    pub token: String,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authentication storage error")]
    Storage(#[from] rusqlite::Error),
    #[error("authentication storage lock poisoned")]
    LockPoisoned,
    #[error("invalid role")]
    InvalidRole,
    #[error("user not found")]
    UserNotFound,
    #[error("token not found")]
    TokenNotFound,
    #[error("username already exists")]
    UsernameExists,
    #[error("bootstrap credential is invalid")]
    InvalidBootstrapCredential,
    #[error("bootstrap is no longer available")]
    BootstrapUnavailable,
    #[error("invalid username")]
    InvalidUsername,
    #[error("expiration must be in the future")]
    InvalidExpiration,
    #[error("system clock is before the Unix epoch")]
    InvalidClock,
    #[error("cryptographic random generator failed")]
    Randomness,
    #[error("filesystem error")]
    Filesystem(#[from] std::io::Error),
}

pub struct AuthStore {
    connection: Mutex<Connection>,
}

impl AuthStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuthError> {
        let path = path.as_ref();
        let memory_database = path == Path::new(":memory:");
        if !memory_database {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                let created = !parent.exists();
                std::fs::create_dir_all(parent)?;
                if created {
                    restrict_directory_permissions(parent)?;
                }
            }
        }
        let connection = Connection::open(path)?;
        if !memory_database {
            restrict_file_permissions(path)?;
        }
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS users (
                 id TEXT PRIMARY KEY NOT NULL,
                 username TEXT NOT NULL UNIQUE,
                 role TEXT NOT NULL CHECK (role IN ('readonly', 'readwrite', 'admin')),
                 enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS tokens (
                 id TEXT PRIMARY KEY NOT NULL,
                 user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                 token_hash BLOB NOT NULL UNIQUE,
                 expires_at INTEGER,
                 revoked INTEGER NOT NULL CHECK (revoked IN (0, 1)),
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tokens_user_id_idx ON tokens(user_id);
             CREATE TABLE IF NOT EXISTS bootstrap_credentials (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 token_hash BLOB NOT NULL
             );
             PRAGMA user_version = 1;",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn ensure_bootstrap_credential(&self) -> Result<Option<String>, AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let has_admin: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')",
            [],
            |row| row.get(0),
        )?;
        let has_bootstrap: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM bootstrap_credentials WHERE id = 1)",
            [],
            |row| row.get(0),
        )?;
        if has_admin || has_bootstrap {
            return Ok(None);
        }
        let credential = generate_secret("neb_boot_")?;
        connection.execute(
            "INSERT INTO bootstrap_credentials(id, token_hash) VALUES (1, ?1)",
            [hash_token(&credential)],
        )?;
        Ok(Some(credential))
    }

    pub fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<(User, CreatedToken), AuthError> {
        validate_username(username)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let transaction = connection.transaction()?;
        let expected: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT token_hash FROM bootstrap_credentials WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let Some(expected) = expected else {
            return Err(AuthError::BootstrapUnavailable);
        };
        if expected != hash_token(credential) {
            return Err(AuthError::InvalidBootstrapCredential);
        }
        let existing_admin: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')",
            [],
            |row| row.get(0),
        )?;
        if existing_admin {
            transaction.execute("DELETE FROM bootstrap_credentials WHERE id = 1", [])?;
            transaction.commit()?;
            return Err(AuthError::BootstrapUnavailable);
        }
        let user = new_user(username, Role::Admin)?;
        transaction.execute(
            "INSERT INTO users(id, username, role, enabled, created_at) VALUES (?1, ?2, ?3, 1, ?4)",
            params![user.id, user.username, user.role.as_str(), user.created_at],
        ).map_err(map_username_constraint)?;
        let token = generate_secret("neb_live_")?;
        let metadata = TokenMetadata {
            id: generate_id()?,
            user_id: user.id.clone(),
            created_at: unix_time()?,
            expires_at: None,
            revoked: false,
        };
        transaction.execute(
            "INSERT INTO tokens(id, user_id, token_hash, expires_at, revoked, created_at) VALUES (?1, ?2, ?3, NULL, 0, ?4)",
            params![metadata.id, metadata.user_id, hash_token(&token), metadata.created_at],
        )?;
        transaction.execute("DELETE FROM bootstrap_credentials WHERE id = 1", [])?;
        transaction.commit()?;
        Ok((user, CreatedToken { metadata, token }))
    }

    pub fn create_user(&self, username: &str, role: Role) -> Result<User, AuthError> {
        validate_username(username)?;
        let user = new_user(username, role)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        connection.execute(
            "INSERT INTO users(id, username, role, enabled, created_at) VALUES (?1, ?2, ?3, 1, ?4)",
            params![user.id, user.username, user.role.as_str(), user.created_at],
        ).map_err(map_username_constraint)?;
        Ok(user)
    }

    pub fn list_users(&self) -> Result<Vec<User>, AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, username, role, enabled, created_at FROM users ORDER BY username",
        )?;
        let users = statement
            .query_map([], read_user)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(users)
    }

    pub fn get_user(&self, username: &str) -> Result<Option<User>, AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        connection
            .query_row(
                "SELECT id, username, role, enabled, created_at FROM users WHERE username = ?1",
                [username],
                read_user,
            )
            .optional()
            .map_err(AuthError::Storage)
    }

    pub fn set_role(&self, username: &str, role: Role) -> Result<(), AuthError> {
        self.update_user(username, "role", role.as_str())
    }

    pub fn set_enabled(&self, username: &str, enabled: bool) -> Result<(), AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let changed = connection.execute(
            "UPDATE users SET enabled = ?1 WHERE username = ?2",
            params![enabled, username],
        )?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }

    pub fn delete_user(&self, username: &str) -> Result<(), AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let changed = connection.execute("DELETE FROM users WHERE username = ?1", [username])?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }

    pub fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, AuthError> {
        let now = unix_time()?;
        if expires_at.is_some_and(|expires_at| expires_at <= now) {
            return Err(AuthError::InvalidExpiration);
        }
        let token = generate_secret("neb_live_")?;
        let hash = hash_token(&token);
        let metadata = TokenMetadata {
            id: generate_id()?,
            user_id: String::new(),
            created_at: now,
            expires_at,
            revoked: false,
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let user_id: Option<String> = connection
            .query_row(
                "SELECT id FROM users WHERE username = ?1 AND enabled = 1",
                [username],
                |row| row.get(0),
            )
            .optional()?;
        let Some(user_id) = user_id else {
            return Err(AuthError::UserNotFound);
        };
        connection.execute(
            "INSERT INTO tokens(id, user_id, token_hash, expires_at, revoked, created_at) VALUES (?1, ?2, ?3, ?4, 0, ?5)",
            params![metadata.id, user_id, hash, expires_at, now],
        )?;
        Ok(CreatedToken {
            metadata: TokenMetadata {
                user_id,
                ..metadata
            },
            token,
        })
    }

    pub fn list_tokens(&self, username: &str) -> Result<Vec<TokenMetadata>, AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let mut statement = connection.prepare(
            "SELECT t.id, t.user_id, t.created_at, t.expires_at, t.revoked
             FROM tokens t JOIN users u ON u.id = t.user_id
             WHERE u.username = ?1 ORDER BY t.created_at DESC",
        )?;
        let rows = statement.query_map([username], |row| {
            Ok(TokenMetadata {
                id: row.get(0)?,
                user_id: row.get(1)?,
                created_at: row.get(2)?,
                expires_at: row.get(3)?,
                revoked: row.get(4)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(AuthError::Storage)
    }

    pub fn revoke_token(&self, token_id: &str) -> Result<(), AuthError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let changed =
            connection.execute("UPDATE tokens SET revoked = 1 WHERE id = ?1", [token_id])?;
        if changed == 0 {
            return Err(AuthError::TokenNotFound);
        }
        Ok(())
    }

    pub fn authenticate(&self, token: &str) -> Result<Option<AuthenticatedIdentity>, AuthError> {
        let now = unix_time()?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        connection
            .query_row(
                "SELECT u.id, u.username, u.role
                 FROM tokens t JOIN users u ON u.id = t.user_id
                 WHERE t.token_hash = ?1 AND t.revoked = 0 AND u.enabled = 1
                   AND (t.expires_at IS NULL OR t.expires_at > ?2)",
                params![hash_token(token), now],
                |row| {
                    let role: String = row.get(2)?;
                    Ok(AuthenticatedIdentity {
                        user_id: row.get(0)?,
                        username: row.get(1)?,
                        role: Role::try_from(role.as_str()).map_err(|_| {
                            rusqlite::Error::InvalidColumnType(
                                2,
                                "role".into(),
                                rusqlite::types::Type::Text,
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(AuthError::Storage)
    }

    fn update_user(&self, username: &str, column: &str, value: &str) -> Result<(), AuthError> {
        let query = format!("UPDATE users SET {column} = ?1 WHERE username = ?2");
        let connection = self
            .connection
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?;
        let changed = connection.execute(&query, params![value, username])?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }
}

fn read_user(row: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    let role: String = row.get(2)?;
    let role = Role::try_from(role.as_str()).map_err(|_| {
        rusqlite::Error::InvalidColumnType(2, "role".into(), rusqlite::types::Type::Text)
    })?;
    Ok(User {
        id: row.get(0)?,
        username: row.get(1)?,
        role,
        enabled: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn validate_username(username: &str) -> Result<(), AuthError> {
    if username.trim().is_empty() || username.len() > 128 {
        return Err(AuthError::InvalidUsername);
    }
    Ok(())
}

fn new_user(username: &str, role: Role) -> Result<User, AuthError> {
    Ok(User {
        id: generate_id()?,
        username: username.to_owned(),
        role,
        enabled: true,
        created_at: unix_time()?,
    })
}

fn generate_id() -> Result<String, AuthError> {
    generate_secret("")
}

fn generate_secret(prefix: &str) -> Result<String, AuthError> {
    let mut bytes = [0_u8; 32];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| AuthError::Randomness)?;
    let mut result = String::with_capacity(prefix.len() + bytes.len() * 2);
    result.push_str(prefix);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(result, "{byte:02x}").map_err(|_| AuthError::Randomness)?;
    }
    Ok(result)
}

fn hash_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

fn unix_time() -> Result<i64, AuthError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .map_err(|_| AuthError::InvalidClock)
}

fn map_username_constraint(error: rusqlite::Error) -> AuthError {
    if matches!(
        &error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
    ) {
        AuthError::UsernameExists
    } else {
        AuthError::Storage(error)
    }
}

#[cfg(unix)]
fn restrict_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn restrict_directory_permissions(_: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_file_permissions(_: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> AuthStore {
        AuthStore::open(":memory:").unwrap()
    }

    #[test]
    fn token_secret_is_shown_once_and_authenticates() {
        let store = store();
        let user = store.create_user("service", Role::ReadWrite).unwrap();
        let token = store.create_token("service", None).unwrap();
        assert!(token.token.starts_with("neb_live_"));
        assert_eq!(
            store.authenticate(&token.token).unwrap().unwrap().user_id,
            user.id
        );
        assert_eq!(store.authenticate("invalid").unwrap(), None);
    }

    #[test]
    fn revoked_expired_and_disabled_credentials_are_rejected() {
        let store = store();
        store.create_user("service", Role::ReadOnly).unwrap();
        let token = store.create_token("service", None).unwrap();
        store.revoke_token(&token.metadata.id).unwrap();
        assert_eq!(store.authenticate(&token.token).unwrap(), None);
        let token = store.create_token("service", None).unwrap();
        store.set_enabled("service", false).unwrap();
        assert_eq!(store.authenticate(&token.token).unwrap(), None);
    }

    #[test]
    fn bootstrap_credential_is_one_time() {
        let store = store();
        let credential = store.ensure_bootstrap_credential().unwrap().unwrap();
        let (user, token) = store.bootstrap_admin(&credential, "root").unwrap();
        assert_eq!(user.role, Role::Admin);
        assert_eq!(
            store.authenticate(&token.token).unwrap().unwrap().role,
            Role::Admin
        );
        assert_eq!(
            store
                .bootstrap_admin(&credential, "other")
                .unwrap_err()
                .to_string(),
            "bootstrap is no longer available"
        );
        assert!(store.ensure_bootstrap_credential().unwrap().is_none());
    }

    #[test]
    fn roles_grant_the_expected_permissions() {
        assert!(Role::ReadOnly.can_read());
        assert!(!Role::ReadOnly.can_write());
        assert!(Role::ReadWrite.can_write());
        assert!(Role::Admin.is_admin());
    }
}
