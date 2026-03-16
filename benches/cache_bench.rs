use criterion::{Criterion, black_box, criterion_group, criterion_main, BenchmarkId};
use himura_cache::cache::LruCache;
use himura_cache::resp::{RespParser, RespValue, serialize};
use himura_cache::sharded_cache::ShardedCache;
use std::sync::Arc;
use std::thread;

fn bench_cache_set(c: &mut Criterion) {
    let cache = LruCache::new(1024 * 1024);

    c.bench_function("cache_set", |b| {
        b.iter(|| {
            cache.set(black_box(b"key".to_vec()), black_box(b"value".to_vec()));
        })
    });
}

fn bench_cache_get(c: &mut Criterion) {
    let cache = LruCache::new(1024 * 1024);
    cache.set(b"key".to_vec(), b"value".to_vec());

    c.bench_function("cache_get", |b| {
        b.iter(|| {
            cache.get(black_box(b"key"));
        })
    });
}

fn bench_cache_get_miss(c: &mut Criterion) {
    let cache = LruCache::new(1024 * 1024);

    c.bench_function("cache_get_miss", |b| {
        b.iter(|| {
            cache.get(black_box(b"nonexistent"));
        })
    });
}

fn bench_resp_parse_simple_string(c: &mut Criterion) {
    let data = b"+OK\r\n";

    c.bench_function("resp_parse_simple_string", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(data);
            parser.parse_one().unwrap();
        })
    });
}

fn bench_resp_parse_bulk_string(c: &mut Criterion) {
    let data = b"$11\r\nhello world\r\n";

    c.bench_function("resp_parse_bulk_string", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(data);
            parser.parse_one().unwrap();
        })
    });
}

fn bench_resp_parse_array(c: &mut Criterion) {
    let data = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";

    c.bench_function("resp_parse_array", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(data);
            parser.parse_one().unwrap();
        })
    });
}

fn bench_resp_serialize(c: &mut Criterion) {
    let value = RespValue::BulkString(Some(b"hello world".to_vec()));

    c.bench_function("resp_serialize", |b| {
        b.iter(|| {
            serialize(black_box(&value));
        })
    });
}

fn bench_cache_lru_eviction(c: &mut Criterion) {
    c.bench_function("cache_lru_eviction", |b| {
        b.iter(|| {
            let cache = LruCache::new(100);
            for i in 0..50 {
                cache.set(
                    format!("key{}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                );
            }
        })
    });
}

fn bench_concurrent_lru_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_lru");
    
    for num_threads in [1, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(num_threads), num_threads, |b, &num_threads| {
            let cache = Arc::new(LruCache::new(1024 * 1024));
            
            b.iter(|| {
                let mut handles = vec![];
                
                for t in 0..num_threads {
                    let cache = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("key_{}_{}", t, i).into_bytes();
                            let value = format!("value_{}_{}", t, i).into_bytes();
                            cache.set(key.clone(), value);
                            let _ = cache.get(&key);
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn bench_concurrent_sharded_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_sharded");
    
    for num_threads in [1, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(num_threads), num_threads, |b, &num_threads| {
            let cache = Arc::new(ShardedCache::new(1024 * 1024));
            
            b.iter(|| {
                let mut handles = vec![];
                
                for t in 0..num_threads {
                    let cache = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("key_{}_{}", t, i).into_bytes();
                            let value = format!("value_{}_{}", t, i).into_bytes();
                            cache.set(key.clone(), value);
                            let _ = cache.get(&key);
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn bench_mixed_workload_lru(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload_lru");
    
    for num_threads in [1, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(num_threads), num_threads, |b, &num_threads| {
            let cache = Arc::new(LruCache::new(1024 * 1024));
            
            for i in 0..1000 {
                cache.set(
                    format!("key{}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                );
            }
            
            b.iter(|| {
                let mut handles = vec![];
                
                for t in 0..num_threads {
                    let cache = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("key{}", i % 1000).into_bytes();
                            if i % 10 < 7 {
                                let _ = cache.get(&key);
                            } else {
                                let value = format!("value_{}_{}", t, i).into_bytes();
                                cache.set(key, value);
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn bench_mixed_workload_sharded(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload_sharded");
    
    for num_threads in [1, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(num_threads), num_threads, |b, &num_threads| {
            let cache = Arc::new(ShardedCache::new(1024 * 1024));
            
            for i in 0..1000 {
                cache.set(
                    format!("key{}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                );
            }
            
            b.iter(|| {
                let mut handles = vec![];
                
                for t in 0..num_threads {
                    let cache = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("key{}", i % 1000).into_bytes();
                            if i % 10 < 7 {
                                let _ = cache.get(&key);
                            } else {
                                let value = format!("value_{}_{}", t, i).into_bytes();
                                cache.set(key, value);
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn bench_memory_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pressure");
    
    for max_memory in [1024, 4096, 16384].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(max_memory), max_memory, |b, &max_memory| {
            b.iter(|| {
                let cache = LruCache::new(max_memory);
                for i in 0..1000 {
                    cache.set(
                        format!("key_{}", i).into_bytes(),
                        vec![b'x'; 100],
                    );
                }
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_cache_set,
    bench_cache_get,
    bench_cache_get_miss,
    bench_cache_lru_eviction,
    bench_resp_parse_simple_string,
    bench_resp_parse_bulk_string,
    bench_resp_parse_array,
    bench_resp_serialize,
    bench_concurrent_lru_cache,
    bench_concurrent_sharded_cache,
    bench_mixed_workload_lru,
    bench_mixed_workload_sharded,
    bench_memory_pressure,
);

criterion_main!(benches);
