use std::{error::Error, io, process};

use clap::{Parser, Subcommand};

mod client;

use client::Client;
use tokio::io::{self as tokio_io, AsyncWriteExt};

#[derive(Debug, Parser)]
#[command(
    name = "nebulactl",
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
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nebulactl: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let client = Client::new(cli.address);

    match cli.command {
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
    }
    Ok(())
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
            "nebulactl",
            "set",
            "--address",
            "cache:7002",
            "key",
            "value",
            "60",
        ])
        .unwrap();

        assert_eq!(cli.address, "cache:7002");
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
        let cli = Cli::try_parse_from(["nebulactl", "set", "key", "value"]).unwrap();

        assert!(matches!(cli.command, Command::Set { ttl_secs: 0, .. }));
    }

    #[test]
    fn rejects_invalid_ttl() {
        assert!(Cli::try_parse_from(["nebulactl", "set", "key", "value", "forever"]).is_err());
    }
}
