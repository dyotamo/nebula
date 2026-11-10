use std::{env, error::Error, io, path::PathBuf, process};

use nebulacache::{Config, NebulaCache};

const USAGE: &str = "Usage: cache_cli [--address HOST:PORT] [--tls-ca PEM] <ping|set|get|delete> [arguments]\n\nCommands:\n  ping\n  set <key> <value> [--ttl SECONDS]\n  get <key>\n  delete <key>\n\nEnvironment:\n  NEBULA_ADDR      Server address (default: 127.0.0.1:17171)\n  NEBULA_TLS_CA    PEM CA file; enables TLS\n  NEBULA_TOKEN     API token for authenticated servers (TLS is optional)";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nebulacache-example: {error}");
        eprintln!("{USAGE}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let mut config = Config::default();
    if let Ok(address) = env::var("NEBULA_ADDR") {
        config.address = address;
    }
    if let Ok(path) = env::var("NEBULA_TLS_CA") {
        config.tls_ca_path = Some(PathBuf::from(path));
    }
    config.token = env::var("NEBULA_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());

    let mut args = env::args().skip(1).peekable();
    while let Some(option) = args.peek().cloned() {
        if option == "--help" || option == "-h" {
            println!("{USAGE}");
            return Ok(());
        }
        if option == "--address" || option == "--tls-ca" {
            args.next();
            let value = args
                .next()
                .ok_or_else(|| invalid_argument(format!("{option} requires a value")))?;
            apply_option(&mut config, &option, value)?;
            continue;
        }
        if let Some((name, value)) = option.split_once('=') {
            if name == "--address" || name == "--tls-ca" {
                args.next();
                apply_option(&mut config, name, value.to_owned())?;
                continue;
            }
        }
        break;
    }

    let command = args
        .next()
        .ok_or_else(|| invalid_argument("a command is required"))?;
    let command_args: Vec<String> = args.collect();
    let cache = NebulaCache::with_config(config)?;
    let result: Result<(), Box<dyn Error>> = async {
        match command.as_str() {
            "ping" => {
                require_count(&command_args, 0, "ping")?;
                cache.ping().await?;
                println!("PONG");
            }
            "set" => {
                if command_args.len() != 2 && command_args.len() != 4 {
                    return Err(invalid_argument(
                        "set requires a key and value, with optional --ttl SECONDS",
                    )
                    .into());
                }
                let ttl_secs = if command_args.len() == 4 && command_args[2] == "--ttl" {
                    command_args[3]
                        .parse::<u64>()
                        .map_err(|_| invalid_argument("TTL must be an unsigned 64-bit integer"))?
                } else if command_args.len() == 2 {
                    0
                } else {
                    return Err(invalid_argument("expected --ttl SECONDS").into());
                };
                cache
                    .set_string_with_ttl(&command_args[0], &command_args[1], ttl_secs)
                    .await?;
                println!("stored");
            }
            "get" => {
                require_count(&command_args, 1, "get")?;
                match cache.get(&command_args[0]).await? {
                    Some(value) => match String::from_utf8(value) {
                        Ok(text) => println!("{text}"),
                        Err(error) => println!("{}", encode_hex(&error.into_bytes())),
                    },
                    None => println!("not found"),
                }
            }
            "delete" => {
                require_count(&command_args, 1, "delete")?;
                println!(
                    "{}",
                    if cache.delete(&command_args[0]).await? {
                        "deleted"
                    } else {
                        "not found"
                    }
                );
            }
            _ => return Err(invalid_argument(format!("unknown command: {command}")).into()),
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    cache.close().await;
    result
}

fn apply_option(config: &mut Config, name: &str, value: String) -> Result<(), io::Error> {
    if value.is_empty() {
        return Err(invalid_argument(format!("{name} requires a value")));
    }
    match name {
        "--address" => config.address = value,
        "--tls-ca" => config.tls_ca_path = Some(PathBuf::from(value)),
        _ => return Err(invalid_argument(format!("unknown option: {name}"))),
    }
    Ok(())
}

fn require_count(args: &[String], count: usize, command: &str) -> Result<(), io::Error> {
    if args.len() != count {
        return Err(invalid_argument(format!(
            "{command} requires {count} argument{}",
            if count == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

fn invalid_argument(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn encode_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
