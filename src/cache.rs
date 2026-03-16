use fxhash::FxHashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const INLINE_SIZE: usize = 24;

#[derive(Clone)]
enum Value {
    Inline { data: [u8; INLINE_SIZE], len: u8 },
    Heap(bytes::Bytes),
}

impl Value {
    fn new(value: Vec<u8>) -> Self {
        if value.len() <= INLINE_SIZE {
            let mut data = [0u8; INLINE_SIZE];
            data[..value.len()].copy_from_slice(&value);
            Value::Inline { data, len: value.len() as u8 }
        } else {
            Value::Heap(bytes::Bytes::from(value))
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Value::Inline { data, len } => &data[..*len as usize],
            Value::Heap(bytes) => bytes,
        }
    }

    fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    fn len(&self) -> usize {
        match self {
            Value::Inline { len, .. } => *len as usize,
            Value::Heap(bytes) => bytes.len(),
        }
    }
}

struct Entry {
    value: Value,
    expires_at: Option<Instant>,
    prev: Option<usize>,
    next: Option<usize>,
}

pub struct LruCache {
    entries: RwLock<FxHashMap<Vec<u8>, usize>>,
    slots: RwLock<Vec<Option<Entry>>>,
    free_list: RwLock<Vec<usize>>,
    head: RwLock<Option<usize>>,
    tail: RwLock<Option<usize>>,
    current_memory: AtomicUsize,
    max_memory: usize,
}

impl LruCache {
    pub fn new(max_memory: usize) -> Self {
        Self {
            entries: RwLock::new(FxHashMap::default()),
            slots: RwLock::new(Vec::new()),
            free_list: RwLock::new(Vec::new()),
            head: RwLock::new(None),
            tail: RwLock::new(None),
            current_memory: AtomicUsize::new(0),
            max_memory,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entries = self.entries.read();
        let &idx = entries.get(key)?;
        drop(entries);

        let (value, expired) = {
            let slots = self.slots.read();
            let entry = slots[idx].as_ref()?;

            let expired = entry.expires_at.map_or(false, |e| e <= Instant::now());
            let value = if !expired { Some(entry.value.to_vec()) } else { None };
            (value, expired)
        };

        if expired {
            drop(self.delete_internal(key));
            return None;
        }

        self.move_to_back(idx);
        value
    }

    pub fn get_with_ttl(&self, key: &[u8]) -> Option<(Vec<u8>, i64)> {
        let entries = self.entries.read();
        let &idx = entries.get(key)?;

        let slots = self.slots.read();
        let entry = slots[idx].as_ref()?;

        let expired = entry.expires_at.map_or(false, |e| e <= Instant::now());
        if expired {
            drop(entries);
            drop(slots);
            drop(self.delete_internal(key));
            return None;
        }

        let ttl_ms = match entry.expires_at {
            Some(expires_at) => {
                let duration = expires_at.duration_since(Instant::now());
                duration.as_millis() as i64
            }
            None => -1,
        };

        Some((entry.value.to_vec(), ttl_ms))
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        self.set_with_ttl(key, value, None);
    }

    pub fn set_with_ttl(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let key_len = key.len();
        let value_len = value.len();
        let new_size = key_len + value_len;

        let mut entries = self.entries.write();
        let mut slots = self.slots.write();
        let mut free_list = self.free_list.write();

        if let Some(&idx) = entries.get(&key) {
            let old_size = key_len + slots[idx].as_ref().unwrap().value.len();
            self.current_memory.fetch_sub(old_size, Ordering::Relaxed);

            if let Some(entry) = &mut slots[idx] {
                entry.value = Value::new(value);
                entry.expires_at = ttl.map(|t| Instant::now() + t);
            }

            self.current_memory.fetch_add(new_size, Ordering::Relaxed);
            self.move_to_back_unlocked(idx, &mut slots);
        } else {
            let idx = if let Some(free_idx) = free_list.pop() {
                free_idx
            } else {
                let idx = slots.len();
                slots.push(None);
                idx
            };

            let mut tail = self.tail.write();
            let mut head = self.head.write();

            slots[idx] = Some(Entry {
                value: Value::new(value),
                expires_at: ttl.map(|t| Instant::now() + t),
                prev: *tail,
                next: None,
            });

            if let Some(old_tail) = *tail {
                if let Some(tail_entry) = &mut slots[old_tail] {
                    tail_entry.next = Some(idx);
                }
            }

            *tail = Some(idx);
            if head.is_none() {
                *head = Some(idx);
            }

            drop(tail);
            drop(head);

            self.current_memory.fetch_add(new_size, Ordering::Relaxed);
            entries.insert(key, idx);
        }

        drop(entries);
        drop(slots);
        drop(free_list);

        while self.current_memory.load(Ordering::Relaxed) > self.max_memory {
            if !self.evict_lru() {
                break;
            }
        }
    }

    pub fn expire(&self, key: &[u8], ttl: Duration) -> bool {
        let entries = self.entries.read();
        let idx = match entries.get(key) {
            Some(&idx) => idx,
            None => return false,
        };
        drop(entries);

        let mut slots = self.slots.write();
        if let Some(entry) = &mut slots[idx] {
            entry.expires_at = Some(Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.delete_internal(key).is_some()
    }

    fn delete_internal(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut entries = self.entries.write();
        let idx = entries.remove(key)?;

        let mut slots = self.slots.write();
        let mut free_list = self.free_list.write();
        let mut head = self.head.write();
        let mut tail = self.tail.write();

        let entry = slots[idx].take()?;
        let size = key.len() + entry.value.len();
        self.current_memory.fetch_sub(size, Ordering::Relaxed);

        if let Some(prev_idx) = entry.prev {
            if let Some(prev_entry) = &mut slots[prev_idx] {
                prev_entry.next = entry.next;
            }
        } else {
            *head = entry.next;
        }

        if let Some(next_idx) = entry.next {
            if let Some(next_entry) = &mut slots[next_idx] {
                next_entry.prev = entry.prev;
            }
        } else {
            *tail = entry.prev;
        }

        free_list.push(idx);
        Some(key.to_vec())
    }

    pub fn clear(&self) {
        self.entries.write().clear();
        self.slots.write().clear();
        self.free_list.write().clear();
        *self.head.write() = None;
        *self.tail.write() = None;
        self.current_memory.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    pub fn memory_usage(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    fn move_to_back(&self, idx: usize) {
        if self.tail.read().as_ref() == Some(&idx) {
            return;
        }

        let mut slots = self.slots.write();
        self.move_to_back_unlocked(idx, &mut slots);
    }

    fn move_to_back_unlocked(&self, idx: usize, slots: &mut [Option<Entry>]) {
        let mut tail = self.tail.write();
        let mut head = self.head.write();

        if tail.as_ref() == Some(&idx) {
            return;
        }

        let (prev_idx, next_idx) = {
            let entry = slots[idx].as_ref().unwrap();
            (entry.prev, entry.next)
        };

        if let Some(prev_idx) = prev_idx {
            if let Some(prev_entry) = &mut slots[prev_idx] {
                prev_entry.next = next_idx;
            }
        } else {
            *head = next_idx;
        }

        if let Some(next_idx) = next_idx {
            if let Some(next_entry) = &mut slots[next_idx] {
                next_entry.prev = prev_idx;
            }
        } else {
            *tail = prev_idx;
        }

        if let Some(entry) = &mut slots[idx] {
            entry.prev = *tail;
            entry.next = None;
        }

        if let Some(old_tail) = *tail {
            if let Some(tail_entry) = &mut slots[old_tail] {
                tail_entry.next = Some(idx);
            }
        }

        *tail = Some(idx);
    }

    fn evict_lru(&self) -> bool {
        let head_idx = match *self.head.read() {
            Some(idx) => idx,
            None => return false,
        };

        let mut entries = self.entries.write();
        let mut slots = self.slots.write();
        let mut free_list = self.free_list.write();
        let mut head = self.head.write();

        let key_to_remove = entries.iter()
            .find(|&(_, &idx)| idx == head_idx)
            .map(|(k, _)| k.clone());

        if let Some(key) = key_to_remove {
            entries.remove(&key);

            let entry = slots[head_idx].take().unwrap();
            let size = key.len() + entry.value.len();
            self.current_memory.fetch_sub(size, Ordering::Relaxed);

            if let Some(next_idx) = entry.next {
                if let Some(next_entry) = &mut slots[next_idx] {
                    next_entry.prev = None;
                }
                *head = Some(next_idx);
            } else {
                *head = None;
                *self.tail.write() = None;
            }

            free_list.push(head_idx);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let cache = LruCache::new(1000);
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_delete() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        assert!(cache.delete(b"key1"));
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let cache = LruCache::new(1000);
        assert!(!cache.delete(b"key1"));
    }

    #[test]
    fn test_update_existing() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key1".to_vec(), b"value2".to_vec());
        assert_eq!(cache.get(b"key1"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_lru_eviction() {
        let cache = LruCache::new(20);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key2".to_vec(), b"value2".to_vec());
        cache.set(b"key3".to_vec(), b"value3".to_vec());

        assert!(cache.get(b"key1").is_none());
        assert!(cache.get(b"key2").is_some());
        assert!(cache.get(b"key3").is_some());
    }

    #[test]
    fn test_clear() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key2".to_vec(), b"value2".to_vec());
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.memory_usage(), 0);
    }

    #[test]
    fn test_memory_tracking() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(cache.memory_usage(), 4 + 6);
    }

    #[test]
    fn test_len() {
        let cache = LruCache::new(1000);
        assert_eq!(cache.len(), 0);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.set(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_expire() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());

        assert!(cache.expire(b"key1", Duration::from_secs(10)));
        assert!(!cache.expire(b"nonexistent", Duration::from_secs(10)));
    }

    #[test]
    fn test_ttl() {
        let cache = LruCache::new(1000);
        cache.set(b"key1".to_vec(), b"value1".to_vec());
        cache.expire(b"key1", Duration::from_secs(10));

        let result = cache.get_with_ttl(b"key1");
        assert!(result.is_some());
        let (_, ttl_ms) = result.unwrap();
        assert!(ttl_ms > 0 && ttl_ms <= 10000);
    }

    #[test]
    fn test_set_with_ttl() {
        let cache = LruCache::new(1000);
        cache.set_with_ttl(b"key1".to_vec(), b"value1".to_vec(), Some(Duration::from_secs(10)));

        assert!(cache.get(b"key1").is_some());

        let result = cache.get_with_ttl(b"key1");
        assert!(result.is_some());
    }

    #[test]
    fn test_key_expiration() {
        let cache = LruCache::new(1000);
        cache.set_with_ttl(b"key1".to_vec(), b"value1".to_vec(), Some(Duration::from_millis(50)));

        assert!(cache.get(b"key1").is_some());

        std::thread::sleep(Duration::from_millis(100));

        assert!(cache.get(b"key1").is_none());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;

        let cache = Arc::new(LruCache::new(1024 * 1024));
        let mut handles = vec![];

        for i in 0..10 {
            let cache = Arc::clone(&cache);
            let handle = std::thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key{}", j).into_bytes();
                    let value = format!("value{}-{}", j, i).into_bytes();
                    cache.set(key.clone(), value);
                    let _ = cache.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(!cache.is_empty());
    }

    #[test]
    fn test_inline_small_values() {
        let cache = LruCache::new(1000);

        // Small value (should be inlined)
        cache.set(b"small".to_vec(), b"tiny".to_vec());
        assert_eq!(cache.get(b"small"), Some(b"tiny".to_vec()));

        // Large value (should be on heap)
        let large_value = vec![b'x'; 100];
        cache.set(b"large".to_vec(), large_value.clone());
        assert_eq!(cache.get(b"large"), Some(large_value));
    }
}
