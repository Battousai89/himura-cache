use crate::aof::AofWriter;
use crate::cache::LruCache;
use crate::command::{execute_command, execute_command_sharded};
use crate::resp::{RespParser, RespValue, serialize};
use crate::sharded_cache::ShardedCache;
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};

const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct Server {
    host: String,
    port: u16,
    max_connections: usize,
    max_memory: usize,
    aof_writer: Option<AofWriter>,
    use_sharding: bool,
}

impl Server {
    pub fn new(
        host: String,
        port: u16,
        max_connections: usize,
        max_memory: usize,
        aof_writer: Option<AofWriter>,
    ) -> Self {
        Self {
            host,
            port,
            max_connections,
            max_memory,
            aof_writer,
            use_sharding: max_memory > 100 * 1024 * 1024,
        }
    }

    pub async fn run(self) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;
        println!("Server listening on {}", addr);

        let semaphore = Arc::new(Semaphore::new(self.max_connections));
        let aof_writer = Arc::new(self.aof_writer);

        if self.use_sharding {
            println!("Using sharded cache with {} shards", 16);
            let cache = Arc::new(ShardedCache::new(self.max_memory));

            loop {
                let permit = semaphore.clone().acquire_owned().await?;
                let (socket, addr) = listener.accept().await?;
                println!("New connection: {}", addr);

                let cache = Arc::clone(&cache);
                let aof_writer = Arc::clone(&aof_writer);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection_sharded(socket, cache, aof_writer).await {
                        eprintln!("Error handling connection from {}: {}", addr, e);
                    }
                    drop(permit);
                });
            }
        } else {
            let cache = Arc::new(LruCache::new(self.max_memory));

            loop {
                let permit = semaphore.clone().acquire_owned().await?;
                let (socket, addr) = listener.accept().await?;
                println!("New connection: {}", addr);

                let cache = Arc::clone(&cache);
                let aof_writer = Arc::clone(&aof_writer);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, cache, aof_writer).await {
                        eprintln!("Error handling connection from {}: {}", addr, e);
                    }
                    drop(permit);
                });
            }
        }
    }
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    cache: Arc<LruCache>,
    aof_writer: Arc<Option<AofWriter>>,
) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 16384];

    loop {
        let read_result = timeout(IDLE_TIMEOUT, reader.read(&mut buf)).await;

        let n = match read_result {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(_)) => break,
            Err(_) => break,
        };

        parser.feed(&buf[..n]);

        loop {
            match parser.parse_one() {
                Ok(Some(RespValue::Array(Some(args)))) => {
                    if let Some(ref writer) = *aof_writer {
                        let _ = writer.write(&args);
                    }

                    let response = execute_command(&args, &cache);
                    let serialized = serialize(&response);
                    writer.write_all(&serialized).await?;
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }

    Ok(())
}

async fn handle_connection_sharded(
    socket: tokio::net::TcpStream,
    cache: Arc<ShardedCache>,
    aof_writer: Arc<Option<AofWriter>>,
) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 16384];

    loop {
        let read_result = timeout(IDLE_TIMEOUT, reader.read(&mut buf)).await;

        let n = match read_result {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(_)) => break,
            Err(_) => break,
        };

        parser.feed(&buf[..n]);

        loop {
            match parser.parse_one() {
                Ok(Some(RespValue::Array(Some(args)))) => {
                    if let Some(ref writer) = *aof_writer {
                        let _ = writer.write(&args);
                    }

                    let response = execute_command_sharded(&args, &cache);
                    let serialized = serialize(&response);
                    writer.write_all(&serialized).await?;
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn send_command(stream: &mut TcpStream, command: &[u8]) -> Vec<u8> {
        stream.write_all(command).await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = vec![0u8; 1024];
        match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await {
            Ok(Ok(n)) => buf[..n].to_vec(),
            _ => vec![],
        }
    }

    #[tokio::test]
    async fn test_server_ping() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let response = send_command(&mut stream, b"*1\r\n$4\r\nPING\r\n").await;
        assert_eq!(response, b"+PONG\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_set_get() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let response = send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n",
        )
        .await;
        assert_eq!(response, b"+OK\r\n");

        let response = send_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
        assert_eq!(response, b"$6\r\nvalue1\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_get_nonexistent() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let response =
            send_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$11\r\nnonexistent\r\n").await;
        assert_eq!(response, b"$-1\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_del() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n",
        )
        .await;

        let response = send_command(&mut stream, b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n").await;
        assert_eq!(response, b":1\r\n");

        let response = send_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
        assert_eq!(response, b"$-1\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_flushall() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n",
        )
        .await;

        let response = send_command(&mut stream, b"*1\r\n$8\r\nFLUSHALL\r\n").await;
        assert_eq!(response, b"+OK\r\n");

        let response = send_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
        assert_eq!(response, b"$-1\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_empty_value() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let response =
            send_command(&mut stream, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$0\r\n\r\n").await;
        assert_eq!(response, b"+OK\r\n");

        let response = send_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
        assert_eq!(response, b"$0\r\n\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_special_chars() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let response = send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$5\r\nhello\r\n",
        )
        .await;
        assert_eq!(response, b"+OK\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_del_multiple() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n",
        )
        .await;
        send_command(
            &mut stream,
            b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n",
        )
        .await;

        let response = send_command(
            &mut stream,
            b"*3\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n",
        )
        .await;
        assert_eq!(response, b":2\r\n");

        drop(stream);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_server_unknown_command() {
        let cache = Arc::new(LruCache::new(1024 * 1024));
        let aof_writer: Arc<Option<AofWriter>> = Arc::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let cache_clone = Arc::clone(&cache);
        let aof_clone = Arc::clone(&aof_writer);
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, cache_clone, aof_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let response = send_command(&mut stream, b"*1\r\n$7\r\nUNKNOWN\r\n").await;
        assert!(response.starts_with(b"-ERR"));

        drop(stream);
        server_handle.abort();
    }
}
