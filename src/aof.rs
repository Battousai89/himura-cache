use crate::cache::LruCache;
use crate::resp::{RespParser, RespValue, serialize};
use anyhow::{Context, Result};
use std::path::Path;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

enum AofMessage {
    Write(Vec<u8>),
    Flush,
}

pub struct AofWriter {
    sender: Option<UnboundedSender<AofMessage>>,
    _handle: Option<tokio::task::JoinHandle<()>>,
    enabled: bool,
}

impl AofWriter {
    pub async fn new(path: &str, enabled: bool) -> Result<Self> {
        if !enabled {
            return Ok(Self {
                sender: None,
                _handle: None,
                enabled: false,
            });
        }

        if let Some(parent) = Path::new(path).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create AOF directory: {:?}", parent))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("Failed to open AOF file: {}", path))?;

        let (tx, rx): (UnboundedSender<AofMessage>, UnboundedReceiver<AofMessage>) = mpsc::unbounded_channel();
        
        let handle = tokio::spawn(async move {
            Self::writer_loop(rx, BufWriter::new(file)).await;
        });

        Ok(Self {
            sender: Some(tx),
            _handle: Some(handle),
            enabled: true,
        })
    }

    async fn writer_loop(
        mut rx: UnboundedReceiver<AofMessage>,
        mut file: BufWriter<File>,
    ) {
        use tokio::time::{Duration, interval};
        const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
        
        let mut flush_interval = interval(FLUSH_INTERVAL);
        
        loop {
            tokio::select! {
                biased;
                
                msg = rx.recv() => {
                    match msg {
                        Some(AofMessage::Write(data)) => {
                            let _ = file.write_all(&data).await;
                        }
                        Some(AofMessage::Flush) => {
                            let _ = file.flush().await;
                            let _ = file.get_mut().sync_all().await;
                        }
                        None => {
                            let _ = file.flush().await;
                            break;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    let _ = file.flush().await;
                }
            }
        }
    }

    pub fn write(&self, command: &[RespValue]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let value = RespValue::Array(Some(command.to_vec()));
        let serialized = serialize(&value);

        if let Some(ref sender) = self.sender {
            let _ = sender.send(AofMessage::Write(serialized));
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(ref sender) = self.sender {
            let _ = sender.send(AofMessage::Flush);
        }
        Ok(())
    }
}

pub fn replay_aof(path: &str, cache: &LruCache) -> Result<usize> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to open AOF file: {}", path))?;

    let mut parser = RespParser::new();
    let mut commands_replayed = 0;
    parser.feed(content.as_bytes());

    loop {
        match parser.parse_one() {
            Ok(Some(RespValue::Array(Some(args)))) => {
                if !args.is_empty() {
                    let command_name = match &args[0] {
                        RespValue::BulkString(Some(cmd)) => cmd.to_ascii_uppercase(),
                        _ => break,
                    };

                    if command_name == b"SET" && args.len() >= 3 {
                        if let (
                            RespValue::BulkString(Some(key)),
                            RespValue::BulkString(Some(value)),
                        ) = (&args[1], &args[2])
                        {
                            cache.set(key.clone(), value.clone());
                            commands_replayed += 1;
                        }
                    } else if command_name == b"DEL" {
                        for arg in &args[1..] {
                            if let RespValue::BulkString(Some(key)) = arg {
                                cache.delete(key);
                            }
                        }
                        commands_replayed += 1;
                    }
                }
            }
            Ok(None) | Err(_) => break,
            _ => break,
        }
    }

    Ok(commands_replayed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> String {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("./test_aof_{}_{}.log", std::process::id(), id)
    }

    #[tokio::test]
    async fn test_aof_write_async() {
        let path = temp_path();
        let writer = AofWriter::new(&path, true).await.unwrap();

        let command = vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key1".to_vec())),
            RespValue::BulkString(Some(b"value1".to_vec())),
        ];

        writer.write(&command).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Give time for async write to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("SET"));
        assert!(content.contains("key1"));
        assert!(content.contains("value1"));

        fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn test_aof_disabled() {
        let path = temp_path();
        let writer = AofWriter::new(&path, false).await.unwrap();

        let command = vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key1".to_vec())),
        ];
        writer.write(&command).unwrap();
        drop(writer);

        assert!(!Path::new(&path).exists());
    }
}
