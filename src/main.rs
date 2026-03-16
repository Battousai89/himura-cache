pub mod aof;
pub mod cache;
pub mod command;
pub mod config;
pub mod resp;
pub mod server;
pub mod sharded_cache;

use anyhow::Result;
use config::Config;
use server::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_file_or_default("config.toml");

    let max_memory = config.parse_max_memory()?;
    println!(
        "Starting Himura Cache on {}:{}",
        config.server.host, config.server.port
    );
    println!("Max memory: {}", config.cache.max_memory);
    println!("AOF enabled: {}", config.aof.enabled);

    let aof_writer = if config.aof.enabled {
        let path = &config.aof.path;

        let cache = cache::LruCache::new(max_memory);
        if std::path::Path::new(path).exists() {
            match aof::replay_aof(path, &cache) {
                Ok(count) => println!("Replayed {} commands from AOF", count),
                Err(e) => eprintln!("Failed to replay AOF: {}", e),
            }
        }
        drop(cache);

        Some(aof::AofWriter::new(path, true).await?)
    } else {
        None
    };

    let server = Server::new(
        config.server.host,
        config.server.port,
        config.server.max_connections,
        max_memory,
        aof_writer,
    );

    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
        }
    }

    Ok(())
}
