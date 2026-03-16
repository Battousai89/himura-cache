use crate::cache::LruCache;
use std::sync::RwLock;
use std::time::Duration;

const NUM_SHARDS: usize = 16;

pub struct ShardedCache {
    shards: Vec<RwLock<LruCache>>,
    max_memory_per_shard: usize,
}

impl ShardedCache {
    pub fn new(max_memory: usize) -> Self {
        let max_memory_per_shard = max_memory / NUM_SHARDS;
        let shards: Vec<RwLock<LruCache>> = (0..NUM_SHARDS)
            .map(|_| RwLock::new(LruCache::new(max_memory_per_shard)))
            .collect();

        Self {
            shards,
            max_memory_per_shard,
        }
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read().unwrap();
        shard.get(key)
    }

    pub fn get_with_ttl(&self, key: &[u8]) -> Option<(Vec<u8>, i64)> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read().unwrap();
        shard.get_with_ttl(key)
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let idx = self.shard_index(&key);
        let shard = self.shards[idx].write().unwrap();
        shard.set(key, value);
    }

    pub fn set_with_ttl(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let idx = self.shard_index(&key);
        let shard = self.shards[idx].write().unwrap();
        shard.set_with_ttl(key, value, ttl);
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].write().unwrap();
        shard.delete(key)
    }

    pub fn expire(&self, key: &[u8], ttl: Duration) -> bool {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].write().unwrap();
        shard.expire(key, ttl)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().unwrap().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().unwrap().is_empty())
    }

    pub fn memory_usage(&self) -> usize {
        self.shards.iter().map(|s| s.read().unwrap().memory_usage()).sum()
    }

    pub fn max_memory(&self) -> usize {
        self.max_memory_per_shard * NUM_SHARDS
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().unwrap().clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharded_set_get() {
        let cache = ShardedCache::new(1024 * 1024);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_sharded_delete() {
        let cache = ShardedCache::new(1024 * 1024);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        assert!(cache.delete(b"key1"));
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_sharded_len() {
        let cache = ShardedCache::new(1024 * 1024);
        assert_eq!(cache.len(), 0);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_sharded_clear() {
        let cache = ShardedCache::new(1024 * 1024);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key2".to_vec(), b"value2".to_vec());
        cache.clear();
        assert!(cache.is_empty());
    }
}
