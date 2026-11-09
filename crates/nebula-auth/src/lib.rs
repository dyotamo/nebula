use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::runtime::{Handle, RuntimeFlavor};

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
    #[error("authentication storage error: {0}")]
    Storage(String),
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
    backend: Option<Box<dyn AuthBackend>>,
}

trait AuthBackend: Send + Sync {
    fn requires_runtime_isolation(&self) -> bool {
        false
    }

    fn ensure_bootstrap_credential(&self) -> Result<Option<String>, AuthError>;
    fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<(User, CreatedToken), AuthError>;
    fn create_user(&self, username: &str, role: Role) -> Result<User, AuthError>;
    fn list_users(&self) -> Result<Vec<User>, AuthError>;
    fn get_user(&self, username: &str) -> Result<Option<User>, AuthError>;
    fn set_role(&self, username: &str, role: Role) -> Result<(), AuthError>;
    fn set_enabled(&self, username: &str, enabled: bool) -> Result<(), AuthError>;
    fn delete_user(&self, username: &str) -> Result<(), AuthError>;
    fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, AuthError>;
    fn list_tokens(&self, username: &str) -> Result<Vec<TokenMetadata>, AuthError>;
    fn revoke_token(&self, token_id: &str) -> Result<(), AuthError>;
    fn authenticate(&self, token: &str) -> Result<Option<AuthenticatedIdentity>, AuthError>;
}

struct SqliteAuthStore {
    connection: Mutex<Connection>,
}

impl AuthStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuthError> {
        Ok(Self {
            backend: Some(Box::new(SqliteAuthStore::open(path)?)),
        })
    }

    pub fn open_postgres(connection_string: &str) -> Result<Self, AuthError> {
        let backend = run_blocking(|| PostgresAuthStore::open(connection_string))?;
        Ok(Self {
            backend: Some(Box::new(backend)),
        })
    }

    fn with_backend<T: Send>(&self, operation: impl FnOnce(&dyn AuthBackend) -> T + Send) -> T {
        let backend = self
            .backend
            .as_deref()
            .expect("auth backend is available until AuthStore is dropped");
        if backend.requires_runtime_isolation() {
            run_blocking(|| operation(backend))
        } else {
            operation(backend)
        }
    }

    pub fn ensure_bootstrap_credential(&self) -> Result<Option<String>, AuthError> {
        self.with_backend(|backend| backend.ensure_bootstrap_credential())
    }

    pub fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<(User, CreatedToken), AuthError> {
        self.with_backend(|backend| backend.bootstrap_admin(credential, username))
    }

    pub fn create_user(&self, username: &str, role: Role) -> Result<User, AuthError> {
        self.with_backend(|backend| backend.create_user(username, role))
    }

    pub fn list_users(&self) -> Result<Vec<User>, AuthError> {
        self.with_backend(|backend| backend.list_users())
    }

    pub fn get_user(&self, username: &str) -> Result<Option<User>, AuthError> {
        self.with_backend(|backend| backend.get_user(username))
    }

    pub fn set_role(&self, username: &str, role: Role) -> Result<(), AuthError> {
        self.with_backend(|backend| backend.set_role(username, role))
    }

    pub fn set_enabled(&self, username: &str, enabled: bool) -> Result<(), AuthError> {
        self.with_backend(|backend| backend.set_enabled(username, enabled))
    }

    pub fn delete_user(&self, username: &str) -> Result<(), AuthError> {
        self.with_backend(|backend| backend.delete_user(username))
    }

    pub fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, AuthError> {
        self.with_backend(|backend| backend.create_token(username, expires_at))
    }

    pub fn list_tokens(&self, username: &str) -> Result<Vec<TokenMetadata>, AuthError> {
        self.with_backend(|backend| backend.list_tokens(username))
    }

    pub fn revoke_token(&self, token_id: &str) -> Result<(), AuthError> {
        self.with_backend(|backend| backend.revoke_token(token_id))
    }

    pub fn authenticate(&self, token: &str) -> Result<Option<AuthenticatedIdentity>, AuthError> {
        self.with_backend(|backend| backend.authenticate(token))
    }
}

impl Drop for AuthStore {
    fn drop(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        if backend.requires_runtime_isolation() {
            run_blocking(move || drop(backend));
        } else {
            drop(backend);
        }
    }
}

fn run_blocking<T: Send>(operation: impl FnOnce() -> T + Send) -> T {
    match Handle::try_current() {
        Ok(handle) if matches!(handle.runtime_flavor(), RuntimeFlavor::MultiThread) => {
            tokio::task::block_in_place(operation)
        }
        Ok(_) => std::thread::scope(|scope| match scope.spawn(operation).join() {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }),
        Err(_) => operation(),
    }
}

impl SqliteAuthStore {
    fn open(path: impl AsRef<Path>) -> Result<Self, AuthError> {
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
            .map_err(|error| AuthError::Storage(error.to_string()))
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
            .map_err(|error| AuthError::Storage(error.to_string()))
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
            .map_err(|error| AuthError::Storage(error.to_string()))
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

impl AuthBackend for SqliteAuthStore {
    fn ensure_bootstrap_credential(&self) -> Result<Option<String>, AuthError> {
        SqliteAuthStore::ensure_bootstrap_credential(self)
    }
    fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<(User, CreatedToken), AuthError> {
        SqliteAuthStore::bootstrap_admin(self, credential, username)
    }
    fn create_user(&self, username: &str, role: Role) -> Result<User, AuthError> {
        SqliteAuthStore::create_user(self, username, role)
    }
    fn list_users(&self) -> Result<Vec<User>, AuthError> {
        SqliteAuthStore::list_users(self)
    }
    fn get_user(&self, username: &str) -> Result<Option<User>, AuthError> {
        SqliteAuthStore::get_user(self, username)
    }
    fn set_role(&self, username: &str, role: Role) -> Result<(), AuthError> {
        SqliteAuthStore::set_role(self, username, role)
    }
    fn set_enabled(&self, username: &str, enabled: bool) -> Result<(), AuthError> {
        SqliteAuthStore::set_enabled(self, username, enabled)
    }
    fn delete_user(&self, username: &str) -> Result<(), AuthError> {
        SqliteAuthStore::delete_user(self, username)
    }
    fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, AuthError> {
        SqliteAuthStore::create_token(self, username, expires_at)
    }
    fn list_tokens(&self, username: &str) -> Result<Vec<TokenMetadata>, AuthError> {
        SqliteAuthStore::list_tokens(self, username)
    }
    fn revoke_token(&self, token_id: &str) -> Result<(), AuthError> {
        SqliteAuthStore::revoke_token(self, token_id)
    }
    fn authenticate(&self, token: &str) -> Result<Option<AuthenticatedIdentity>, AuthError> {
        SqliteAuthStore::authenticate(self, token)
    }
}

struct PostgresAuthStore {
    client: Mutex<Client>,
}

impl PostgresAuthStore {
    fn open(connection_string: &str) -> Result<Self, AuthError> {
        let tls = native_tls::TlsConnector::new().map_err(storage_error)?;
        let mut client = Client::connect(connection_string, MakeTlsConnector::new(tls))
            .map_err(storage_error)?;
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS users (
                 id TEXT PRIMARY KEY NOT NULL,
                 username TEXT NOT NULL UNIQUE,
                 role TEXT NOT NULL CHECK (role IN ('readonly', 'readwrite', 'admin')),
                 enabled BOOLEAN NOT NULL,
                 created_at BIGINT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS tokens (
                 id TEXT PRIMARY KEY NOT NULL,
                 user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                 token_hash BYTEA NOT NULL UNIQUE,
                 expires_at BIGINT,
                 revoked BOOLEAN NOT NULL,
                 created_at BIGINT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tokens_user_id_idx ON tokens(user_id);
             CREATE TABLE IF NOT EXISTS bootstrap_credentials (
                 id SMALLINT PRIMARY KEY CHECK (id = 1),
                 token_hash BYTEA NOT NULL
             );",
            )
            .map_err(storage_error)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

impl AuthBackend for PostgresAuthStore {
    fn requires_runtime_isolation(&self) -> bool {
        true
    }

    fn ensure_bootstrap_credential(&self) -> Result<Option<String>, AuthError> {
        let mut client = self.client.lock().map_err(|_| AuthError::LockPoisoned)?;
        let mut transaction = client.transaction().map_err(storage_error)?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock(827100001)", &[])
            .map_err(storage_error)?;
        let has_admin: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')",
                &[],
            )
            .map_err(storage_error)?
            .get(0);
        let has_bootstrap: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM bootstrap_credentials WHERE id = 1)",
                &[],
            )
            .map_err(storage_error)?
            .get(0);
        if has_admin || has_bootstrap {
            transaction.commit().map_err(storage_error)?;
            return Ok(None);
        }
        let credential = generate_secret("neb_boot_")?;
        transaction
            .execute(
                "INSERT INTO bootstrap_credentials(id, token_hash) VALUES (1, $1)",
                &[&hash_token(&credential)],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(Some(credential))
    }

    fn bootstrap_admin(
        &self,
        credential: &str,
        username: &str,
    ) -> Result<(User, CreatedToken), AuthError> {
        validate_username(username)?;
        let mut client = self.client.lock().map_err(|_| AuthError::LockPoisoned)?;
        let mut transaction = client.transaction().map_err(storage_error)?;
        let expected: Option<Vec<u8>> = transaction
            .query_opt(
                "SELECT token_hash FROM bootstrap_credentials WHERE id = 1 FOR UPDATE",
                &[],
            )
            .map_err(storage_error)?
            .map(|row| row.get(0));
        let Some(expected) = expected else {
            return Err(AuthError::BootstrapUnavailable);
        };
        if expected != hash_token(credential) {
            return Err(AuthError::InvalidBootstrapCredential);
        }
        let existing_admin: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM users WHERE role = 'admin')",
                &[],
            )
            .map_err(storage_error)?
            .get(0);
        if existing_admin {
            transaction
                .execute("DELETE FROM bootstrap_credentials WHERE id = 1", &[])
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            return Err(AuthError::BootstrapUnavailable);
        }
        let user = new_user(username, Role::Admin)?;
        transaction.execute("INSERT INTO users(id, username, role, enabled, created_at) VALUES ($1, $2, $3, TRUE, $4)", &[&user.id, &user.username, &user.role.as_str(), &user.created_at]).map_err(map_postgres_username_constraint)?;
        let token = generate_secret("neb_live_")?;
        let metadata = TokenMetadata {
            id: generate_id()?,
            user_id: user.id.clone(),
            created_at: unix_time()?,
            expires_at: None,
            revoked: false,
        };
        transaction.execute("INSERT INTO tokens(id, user_id, token_hash, expires_at, revoked, created_at) VALUES ($1, $2, $3, NULL, FALSE, $4)", &[&metadata.id, &metadata.user_id, &hash_token(&token), &metadata.created_at]).map_err(storage_error)?;
        transaction
            .execute("DELETE FROM bootstrap_credentials WHERE id = 1", &[])
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok((user, CreatedToken { metadata, token }))
    }

    fn create_user(&self, username: &str, role: Role) -> Result<User, AuthError> {
        validate_username(username)?;
        let user = new_user(username, role)?;
        self.client.lock().map_err(|_| AuthError::LockPoisoned)?.execute("INSERT INTO users(id, username, role, enabled, created_at) VALUES ($1, $2, $3, TRUE, $4)", &[&user.id, &user.username, &user.role.as_str(), &user.created_at]).map_err(map_postgres_username_constraint)?;
        Ok(user)
    }

    fn list_users(&self) -> Result<Vec<User>, AuthError> {
        let rows = self
            .client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .query(
                "SELECT id, username, role, enabled, created_at FROM users ORDER BY username",
                &[],
            )
            .map_err(storage_error)?;
        rows.iter().map(postgres_read_user).collect()
    }

    fn get_user(&self, username: &str) -> Result<Option<User>, AuthError> {
        self.client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .query_opt(
                "SELECT id, username, role, enabled, created_at FROM users WHERE username = $1",
                &[&username],
            )
            .map_err(storage_error)?
            .as_ref()
            .map(postgres_read_user)
            .transpose()
    }

    fn set_role(&self, username: &str, role: Role) -> Result<(), AuthError> {
        self.update_user("role", role.as_str(), username)
    }

    fn set_enabled(&self, username: &str, enabled: bool) -> Result<(), AuthError> {
        let changed = self
            .client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .execute(
                "UPDATE users SET enabled = $1 WHERE username = $2",
                &[&enabled, &username],
            )
            .map_err(storage_error)?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }

    fn delete_user(&self, username: &str) -> Result<(), AuthError> {
        let changed = self
            .client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .execute("DELETE FROM users WHERE username = $1", &[&username])
            .map_err(storage_error)?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }

    fn create_token(
        &self,
        username: &str,
        expires_at: Option<i64>,
    ) -> Result<CreatedToken, AuthError> {
        let now = unix_time()?;
        if expires_at.is_some_and(|expires_at| expires_at <= now) {
            return Err(AuthError::InvalidExpiration);
        }
        let token = generate_secret("neb_live_")?;
        let metadata = TokenMetadata {
            id: generate_id()?,
            user_id: String::new(),
            created_at: now,
            expires_at,
            revoked: false,
        };
        let mut client = self.client.lock().map_err(|_| AuthError::LockPoisoned)?;
        let mut transaction = client.transaction().map_err(storage_error)?;
        let user_id: Option<String> = transaction
            .query_opt(
                "SELECT id FROM users WHERE username = $1 AND enabled = TRUE",
                &[&username],
            )
            .map_err(storage_error)?
            .map(|row| row.get(0));
        let Some(user_id) = user_id else {
            return Err(AuthError::UserNotFound);
        };
        transaction.execute("INSERT INTO tokens(id, user_id, token_hash, expires_at, revoked, created_at) VALUES ($1, $2, $3, $4, FALSE, $5)", &[&metadata.id, &user_id, &hash_token(&token), &expires_at, &now]).map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(CreatedToken {
            metadata: TokenMetadata {
                user_id,
                ..metadata
            },
            token,
        })
    }

    fn list_tokens(&self, username: &str) -> Result<Vec<TokenMetadata>, AuthError> {
        let rows = self.client.lock().map_err(|_| AuthError::LockPoisoned)?.query("SELECT t.id, t.user_id, t.created_at, t.expires_at, t.revoked FROM tokens t JOIN users u ON u.id = t.user_id WHERE u.username = $1 ORDER BY t.created_at DESC", &[&username]).map_err(storage_error)?;
        rows.iter()
            .map(|row| {
                Ok(TokenMetadata {
                    id: row.get(0),
                    user_id: row.get(1),
                    created_at: row.get(2),
                    expires_at: row.get(3),
                    revoked: row.get(4),
                })
            })
            .collect()
    }

    fn revoke_token(&self, token_id: &str) -> Result<(), AuthError> {
        let changed = self
            .client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .execute(
                "UPDATE tokens SET revoked = TRUE WHERE id = $1",
                &[&token_id],
            )
            .map_err(storage_error)?;
        if changed == 0 {
            return Err(AuthError::TokenNotFound);
        }
        Ok(())
    }

    fn authenticate(&self, token: &str) -> Result<Option<AuthenticatedIdentity>, AuthError> {
        let now = unix_time()?;
        let row = self.client.lock().map_err(|_| AuthError::LockPoisoned)?.query_opt("SELECT u.id, u.username, u.role FROM tokens t JOIN users u ON u.id = t.user_id WHERE t.token_hash = $1 AND t.revoked = FALSE AND u.enabled = TRUE AND (t.expires_at IS NULL OR t.expires_at > $2)", &[&hash_token(token), &now]).map_err(storage_error)?;
        row.map(|row| {
            Ok(AuthenticatedIdentity {
                user_id: row.get(0),
                username: row.get(1),
                role: Role::try_from(row.get::<_, String>(2).as_str())?,
            })
        })
        .transpose()
    }
}

impl PostgresAuthStore {
    fn update_user(&self, column: &str, value: &str, username: &str) -> Result<(), AuthError> {
        let query = format!("UPDATE users SET {column} = $1 WHERE username = $2");
        let changed = self
            .client
            .lock()
            .map_err(|_| AuthError::LockPoisoned)?
            .execute(&query, &[&value, &username])
            .map_err(storage_error)?;
        if changed == 0 {
            return Err(AuthError::UserNotFound);
        }
        Ok(())
    }
}

fn postgres_read_user(row: &postgres::Row) -> Result<User, AuthError> {
    Ok(User {
        id: row.get(0),
        username: row.get(1),
        role: Role::try_from(row.get::<_, String>(2).as_str())?,
        enabled: row.get(3),
        created_at: row.get(4),
    })
}

fn storage_error(error: impl std::fmt::Display) -> AuthError {
    AuthError::Storage(error.to_string())
}

fn map_postgres_username_constraint(error: postgres::Error) -> AuthError {
    if error.code().is_some_and(|code| code.code() == "23505")
        && error.as_db_error().and_then(|error| error.constraint()) == Some("users_username_key")
    {
        AuthError::UsernameExists
    } else {
        storage_error(error)
    }
}

impl From<rusqlite::Error> for AuthError {
    fn from(error: rusqlite::Error) -> Self {
        storage_error(error)
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
        storage_error(error)
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

    #[tokio::test]
    async fn run_blocking_supports_nested_runtime_on_current_thread_runtime() {
        let result = run_blocking(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { 42 })
        });
        assert_eq!(result, 42);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_blocking_supports_nested_runtime_on_multi_thread_runtime() {
        let result = run_blocking(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { 42 })
        });
        assert_eq!(result, 42);
    }

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
