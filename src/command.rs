use crate::cache::LruCache;
use crate::resp::RespValue;
use crate::sharded_cache::ShardedCache;

pub type CommandResult = RespValue;

pub fn execute_command(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".to_string());
    }

    let command = match &args[0] {
        RespValue::BulkString(Some(cmd)) => cmd.to_ascii_uppercase(),
        _ => return RespValue::Error("ERR invalid command".to_string()),
    };

    match command.as_slice() {
        b"GET" => cmd_get(&args[1..], cache),
        b"SET" => cmd_set(&args[1..], cache),
        b"DEL" => cmd_del(&args[1..], cache),
        b"PING" => cmd_ping(&args[1..]),
        b"INFO" => cmd_info(&args[1..], cache),
        b"FLUSHALL" => cmd_flushall(&args[1..], cache),
        b"EXPIRE" => cmd_expire(&args[1..], cache),
        b"TTL" => cmd_ttl(&args[1..], cache),
        b"SETEX" => cmd_setex(&args[1..], cache),
        _ => RespValue::Error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&command)
        )),
    }
}

pub fn execute_command_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".to_string());
    }

    let command = match &args[0] {
        RespValue::BulkString(Some(cmd)) => cmd.to_ascii_uppercase(),
        _ => return RespValue::Error("ERR invalid command".to_string()),
    };

    match command.as_slice() {
        b"GET" => cmd_get_sharded(&args[1..], cache),
        b"SET" => cmd_set_sharded(&args[1..], cache),
        b"DEL" => cmd_del_sharded(&args[1..], cache),
        b"PING" => cmd_ping(&args[1..]),
        b"INFO" => cmd_info_sharded(&args[1..], cache),
        b"FLUSHALL" => cmd_flushall_sharded(&args[1..], cache),
        b"EXPIRE" => cmd_expire_sharded(&args[1..], cache),
        b"TTL" => cmd_ttl_sharded(&args[1..], cache),
        b"SETEX" => cmd_setex_sharded(&args[1..], cache),
        _ => RespValue::Error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&command)
        )),
    }
}

fn cmd_get(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'get' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    match cache.get(key) {
        Some(value) => RespValue::BulkString(Some(value.to_vec())),
        None => RespValue::nil(),
    }
}

fn cmd_get_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'get' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    match cache.get(key) {
        Some(value) => RespValue::BulkString(Some(value.to_vec())),
        None => RespValue::nil(),
    }
}

fn cmd_set(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'set' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k.clone(),
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let value = match &args[1] {
        RespValue::BulkString(Some(v)) => v.clone(),
        _ => return RespValue::Error("ERR invalid value".to_string()),
    };

    cache.set(key, value);
    RespValue::ok()
}

fn cmd_set_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'set' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k.clone(),
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let value = match &args[1] {
        RespValue::BulkString(Some(v)) => v.clone(),
        _ => return RespValue::Error("ERR invalid value".to_string()),
    };

    cache.set(key, value);
    RespValue::ok()
}

fn cmd_del(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'del' command".to_string());
    }

    let mut deleted = 0;
    for arg in args {
        if let RespValue::BulkString(Some(key)) = arg
            && cache.delete(key)
        {
            deleted += 1;
        }
    }

    RespValue::Integer(deleted as i64)
}

fn cmd_del_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'del' command".to_string());
    }

    let mut deleted = 0;
    for arg in args {
        if let RespValue::BulkString(Some(key)) = arg
            && cache.delete(key)
        {
            deleted += 1;
        }
    }

    RespValue::Integer(deleted as i64)
}

fn cmd_ping(args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        RespValue::pong()
    } else if args.len() == 1 {
        if let RespValue::BulkString(Some(msg)) = &args[0] {
            RespValue::BulkString(Some(msg.clone()))
        } else {
            RespValue::pong()
        }
    } else {
        RespValue::Error("ERR wrong number of arguments for 'ping' command".to_string())
    }
}

fn cmd_info(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if !args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'info' command".to_string());
    }

    let info = format!(
        "# Server\r\nused_memory: {}\r\nmaxmemory: {}\r\nkeys: {}",
        cache.memory_usage(),
        cache.max_memory(),
        cache.len()
    );

    RespValue::BulkString(Some(info.into_bytes()))
}

fn cmd_info_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if !args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'info' command".to_string());
    }

    let info = format!(
        "# Server\r\nused_memory: {}\r\nmaxmemory: {}\r\nkeys: {}",
        cache.memory_usage(),
        cache.max_memory(),
        cache.len()
    );

    RespValue::BulkString(Some(info.into_bytes()))
}

fn cmd_flushall(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if !args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'flushall' command".to_string(),
        );
    }

    cache.clear();
    RespValue::ok()
}

fn cmd_flushall_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if !args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'flushall' command".to_string(),
        );
    }

    cache.clear();
    RespValue::ok()
}

fn cmd_expire(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'expire' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let ttl_secs = match &args[1] {
        RespValue::BulkString(Some(t)) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<u64>().ok()),
        _ => None,
    };

    let Some(ttl_secs) = ttl_secs else {
        return RespValue::Error("ERR invalid TTL".to_string());
    };

    if cache.expire(key, std::time::Duration::from_secs(ttl_secs)) {
        RespValue::Integer(1)
    } else {
        RespValue::Integer(0)
    }
}

fn cmd_expire_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'expire' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let ttl_secs = match &args[1] {
        RespValue::BulkString(Some(t)) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<u64>().ok()),
        _ => None,
    };

    let Some(ttl_secs) = ttl_secs else {
        return RespValue::Error("ERR invalid TTL".to_string());
    };

    if cache.expire(key, std::time::Duration::from_secs(ttl_secs)) {
        RespValue::Integer(1)
    } else {
        RespValue::Integer(0)
    }
}

fn cmd_ttl(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    match cache.get_with_ttl(key) {
        Some((_, ttl_ms)) => RespValue::Integer(ttl_ms / 1000),
        None => RespValue::Integer(-2),
    }
}

fn cmd_ttl_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k,
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    match cache.get_with_ttl(key) {
        Some((_, ttl_ms)) => RespValue::Integer(ttl_ms / 1000),
        None => RespValue::Integer(-2),
    }
}

fn cmd_setex(args: &[RespValue], cache: &LruCache) -> CommandResult {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setex' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k.clone(),
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let ttl_secs = match &args[1] {
        RespValue::BulkString(Some(t)) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<u64>().ok()),
        _ => None,
    };

    let Some(ttl_secs) = ttl_secs else {
        return RespValue::Error("ERR invalid TTL".to_string());
    };

    let value = match &args[2] {
        RespValue::BulkString(Some(v)) => v.clone(),
        _ => return RespValue::Error("ERR invalid value".to_string()),
    };

    cache.set_with_ttl(key, value, Some(std::time::Duration::from_secs(ttl_secs)));
    RespValue::ok()
}

fn cmd_setex_sharded(args: &[RespValue], cache: &ShardedCache) -> CommandResult {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setex' command".to_string());
    }

    let key = match &args[0] {
        RespValue::BulkString(Some(k)) => k.clone(),
        _ => return RespValue::Error("ERR invalid key".to_string()),
    };

    let ttl_secs = match &args[1] {
        RespValue::BulkString(Some(t)) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<u64>().ok()),
        _ => None,
    };

    let Some(ttl_secs) = ttl_secs else {
        return RespValue::Error("ERR invalid TTL".to_string());
    };

    let value = match &args[2] {
        RespValue::BulkString(Some(v)) => v.clone(),
        _ => return RespValue::Error("ERR invalid value".to_string()),
    };

    cache.set_with_ttl(key, value, Some(std::time::Duration::from_secs(ttl_secs)));
    RespValue::ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_cache() -> LruCache {
        LruCache::new(1000)
    }

    #[test]
    fn test_ping() {
        let cache = new_cache();
        let result = execute_command(&[RespValue::BulkString(Some(b"PING".to_vec()))], &cache);
        assert_eq!(result, RespValue::pong());
    }

    #[test]
    fn test_ping_with_message() {
        let cache = new_cache();
        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"PING".to_vec())),
                RespValue::BulkString(Some(b"hello".to_vec())),
            ],
            &cache,
        );
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn test_set_get() {
        let cache = new_cache();
        execute_command(
            &[
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );

        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
            ],
            &cache,
        );

        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));
    }

    #[test]
    fn test_get_nonexistent() {
        let cache = new_cache();
        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"nonexistent".to_vec())),
            ],
            &cache,
        );

        assert_eq!(result, RespValue::nil());
    }

    #[test]
    fn test_del() {
        let cache = new_cache();
        execute_command(
            &[
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );

        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"DEL".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
            ],
            &cache,
        );

        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_del_nonexistent() {
        let cache = new_cache();
        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"DEL".to_vec())),
                RespValue::BulkString(Some(b"nonexistent".to_vec())),
            ],
            &cache,
        );

        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_info() {
        let cache = new_cache();
        execute_command(
            &[
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );

        let result = execute_command(&[RespValue::BulkString(Some(b"INFO".to_vec()))], &cache);

        if let RespValue::BulkString(Some(info)) = result {
            let info_str = String::from_utf8_lossy(&info);
            assert!(info_str.contains("used_memory:"));
            assert!(info_str.contains("maxmemory:"));
            assert!(info_str.contains("keys: 1"));
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_flushall() {
        let cache = new_cache();
        execute_command(
            &[
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );

        let result = execute_command(&[RespValue::BulkString(Some(b"FLUSHALL".to_vec()))], &cache);
        assert_eq!(result, RespValue::ok());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_unknown_command() {
        let cache = new_cache();
        let result = execute_command(&[RespValue::BulkString(Some(b"UNKNOWN".to_vec()))], &cache);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_wrong_arguments() {
        let cache = new_cache();
        let result = execute_command(&[], &cache);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_setex() {
        let cache = new_cache();
        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"SETEX".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"10".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );
        assert_eq!(result, RespValue::ok());
        assert!(cache.get(b"key1").is_some());
    }

    #[test]
    fn test_expire_ttl() {
        let cache = new_cache();

        execute_command(
            &[
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
            ],
            &cache,
        );

        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"EXPIRE".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"10".to_vec())),
            ],
            &cache,
        );
        assert_eq!(result, RespValue::Integer(1));

        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"TTL".to_vec())),
                RespValue::BulkString(Some(b"key1".to_vec())),
            ],
            &cache,
        );
        if let RespValue::Integer(ttl) = result {
            assert!(ttl > 0 && ttl <= 10);
        } else {
            panic!("Expected Integer");
        }
    }

    #[test]
    fn test_ttl_nonexistent() {
        let cache = new_cache();

        let result = execute_command(
            &[
                RespValue::BulkString(Some(b"TTL".to_vec())),
                RespValue::BulkString(Some(b"nonexistent".to_vec())),
            ],
            &cache,
        );
        assert_eq!(result, RespValue::Integer(-2));
    }
}
