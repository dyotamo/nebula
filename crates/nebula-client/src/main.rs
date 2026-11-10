//! Command-line interface for cache operations and administrator management.
//!
//! Cache commands use the binary TCP protocol, while user and token commands use the HTTPS admin
//! API. Global connection options can also be supplied through the corresponding `NEBULA_*` env vars.

use std::{
    error::Error,
    io,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand};

mod admin;
mod client;

use admin::AdminClient;
use client::Client;
use tokio::io::{self as tokio_io, AsyncWriteExt};

#[derive(Debug, Parser)]
#[command(
    name = "nebula-client",
    version,
    about = "Interact with a Nebula cache server"
)]
struct Cli {
    #[arg(
        short,
        long,
        global = true,
        env = "NEBULA_ADDR",
        default_value = "127.0.0.1:7001",
        value_name = "HOST:PORT"
    )]
    address: String,
    #[arg(
        long,
        global = true,
        env = "NEBULA_ADMIN_ADDR",
        default_value = "https://127.0.0.1:7002",
        value_name = "URL"
    )]
    admin_address: String,
    #[arg(long, global = true, env = "NEBULA_TLS_CA", value_name = "PEM")]
    tls_ca: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Check server connectivity")]
    Ping,
    #[command(about = "Store a value, optionally expiring after TTL_SECONDS")]
    Set {
        key: String,
        value: String,
        #[arg(value_name = "TTL_SECONDS", default_value_t = 0)]
        ttl_secs: u64,
    },
    #[command(about = "Retrieve a value by key")]
    Get { key: String },
    #[command(about = "Delete a value by key")]
    Delete { key: String },
    #[command(about = "Manage users")]
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    #[command(about = "Manage API tokens")]
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    #[command(about = "Create the first administrator using the one-time bootstrap credential")]
    BootstrapAdmin { username: String },
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    Create {
        username: String,
        #[arg(long, default_value = "readonly")]
        role: String,
    },
    List,
    Show {
        username: String,
    },
    SetRole {
        username: String,
        #[arg(long)]
        role: String,
    },
    Disable {
        username: String,
    },
    Enable {
        username: String,
    },
    Delete {
        username: String,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    Create {
        username: String,
        #[arg(long)]
        expires_in: Option<String>,
    },
    List {
        username: String,
    },
    Revoke {
        token_id: String,
    },
}

#[tokio::main]
async fn main() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    if let Err(error) = run().await {
        eprintln!("nebula: {error}");
        process::exit(1);
    }
}

/// Parses options and dispatches cache, user-management, and token-management commands.
async fn run() -> Result<(), Box<dyn Error>> {
    let Cli {
        address,
        admin_address,
        tls_ca,
        command,
    } = Cli::parse();
    match command {
        Command::BootstrapAdmin { username } => {
            let credential = std::env::var("NEBULA_BOOTSTRAP_TOKEN")
                .map_err(|_| invalid_input("NEBULA_BOOTSTRAP_TOKEN is required"))?;
            let client = AdminClient::new(&admin_address, tls_ca.as_deref())?;
            let result = client.bootstrap_admin(&credential, &username).await?;
            println!("Created administrator {}", result.user.username);
            println!("API token (shown once): {}", result.token.token);
            Ok(())
        }
        Command::User { command } => {
            let client = authenticated_admin_client(&admin_address, tls_ca.as_deref()).await?;
            let result = manage_user(&client, command).await;
            let logout = client.logout().await;
            result?;
            logout?;
            Ok(())
        }
        Command::Token { command } => {
            let client = authenticated_admin_client(&admin_address, tls_ca.as_deref()).await?;
            let result = manage_token(&client, command).await;
            let logout = client.logout().await;
            result?;
            logout?;
            Ok(())
        }
        command => {
            let client = Client::new(address, tls_ca, std::env::var("NEBULA_TOKEN").ok())?;
            match command {
                Command::Ping => {
                    client.ping().await?;
                    println!("PONG");
                }
                Command::Set {
                    key,
                    value,
                    ttl_secs,
                } => {
                    client
                        .set(key.as_bytes(), value.as_bytes(), ttl_secs)
                        .await?;
                }
                Command::Get { key } => match client.get(key.as_bytes()).await? {
                    Some(value) => {
                        let mut stdout = tokio_io::stdout();
                        stdout.write_all(&value).await?;
                        stdout.write_all(b"\n").await?;
                        stdout.flush().await?;
                    }
                    None => return Err(invalid_input("key not found").into()),
                },
                Command::Delete { key } => {
                    if client.delete(key.as_bytes()).await? {
                        println!("deleted");
                    } else {
                        println!("not found");
                    }
                }
                Command::User { .. } | Command::Token { .. } | Command::BootstrapAdmin { .. } => {
                    unreachable!()
                }
            }
            Ok(())
        }
    }
}

async fn authenticated_admin_client(
    address: &str,
    tls_ca: Option<&std::path::Path>,
) -> Result<AdminClient, Box<dyn Error>> {
    let token = std::env::var("NEBULA_TOKEN")
        .map_err(|_| invalid_input("NEBULA_TOKEN must contain an administrator API token"))?;
    let mut client = AdminClient::new(address, tls_ca)?;
    client.authenticate(&token).await?;
    Ok(client)
}

async fn manage_user(client: &AdminClient, command: UserCommand) -> Result<(), Box<dyn Error>> {
    match command {
        UserCommand::Create { username, role } => {
            print_user(&client.create_user(&username, &role).await?);
        }
        UserCommand::List => {
            for user in client.list_users().await? {
                print_user(&user);
            }
        }
        UserCommand::Show { username } => print_user(&client.show_user(&username).await?),
        UserCommand::SetRole { username, role } => {
            client.set_user_role(&username, &role).await?;
        }
        UserCommand::Disable { username } => {
            client.set_user_enabled(&username, false).await?;
        }
        UserCommand::Enable { username } => {
            client.set_user_enabled(&username, true).await?;
        }
        UserCommand::Delete { username } => client.delete_user(&username).await?,
    }
    Ok(())
}

async fn manage_token(client: &AdminClient, command: TokenCommand) -> Result<(), Box<dyn Error>> {
    match command {
        TokenCommand::Create {
            username,
            expires_in,
        } => {
            let expires_at = expires_in.as_deref().map(expiration_time).transpose()?;
            let token = client.create_token(&username, expires_at).await?;
            println!("Token ID: {}", token.id);
            println!("Token (shown once): {}", token.token);
        }
        TokenCommand::List { username } => {
            for token in client.list_tokens(&username).await? {
                println!(
                    "{} created_at={} expires_at={:?} revoked={}",
                    token.id, token.created_at, token.expires_at, token.revoked
                );
            }
        }
        TokenCommand::Revoke { token_id } => client.revoke_token(&token_id).await?,
    }
    Ok(())
}

fn print_user(user: &admin::User) {
    println!(
        "{} role={} enabled={} created_at={}",
        user.username, user.role, user.enabled, user.created_at
    );
}

/// Converts a relative `s`, `m`, `h`, or `d` duration into an absolute Unix timestamp.
fn expiration_time(value: &str) -> Result<i64, Box<dyn Error>> {
    let (amount, unit) = value.split_at(
        value
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(value.len()),
    );
    let amount = amount
        .parse::<u64>()
        .map_err(|_| invalid_input("invalid token expiration"))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => {
            return Err(invalid_input("expiration must use s, m, h, or d (for example 30d)").into())
        }
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let expires = now
        .checked_add(
            amount
                .checked_mul(multiplier)
                .ok_or_else(|| invalid_input("expiration is too large"))?,
        )
        .ok_or_else(|| invalid_input("expiration is too large"))?;
    Ok(i64::try_from(expires).map_err(|_| invalid_input("expiration is too large"))?)
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn parses_global_address_and_subcommand_arguments() {
        let cli = Cli::try_parse_from([
            "nebula-client",
            "set",
            "--address",
            "cache:7002",
            "--tls-ca",
            "rootCA.pem",
            "key",
            "value",
            "60",
        ])
        .unwrap();

        assert_eq!(cli.address, "cache:7002");
        assert_eq!(
            cli.tls_ca.as_deref().and_then(|path| path.to_str()),
            Some("rootCA.pem")
        );
        assert!(matches!(
            cli.command,
            Command::Set {
                key,
                value,
                ttl_secs: 60
            } if key == "key" && value == "value"
        ));
    }

    #[test]
    fn defaults_set_ttl_to_zero() {
        let cli = Cli::try_parse_from(["nebula-client", "set", "key", "value"]).unwrap();

        assert!(matches!(cli.command, Command::Set { ttl_secs: 0, .. }));
    }

    #[test]
    fn rejects_invalid_ttl() {
        assert!(Cli::try_parse_from(["nebula-client", "set", "key", "value", "forever"]).is_err());
    }
}
