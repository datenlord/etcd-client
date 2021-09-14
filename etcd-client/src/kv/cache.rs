//! The implementation for Etcd cache.
use super::KeyValue;
use super::OverflowArithmetic;
use crate::watch::EtcdWatchRequest;
use crate::Result as Res;
use priority_queue::PriorityQueue;
use smol::channel::Sender;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use utilities::Cast;

/// Cache entry
#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    /// watch id related to key
    watch_id: i64,
    /// current revision of key in cache
    revision: i64,
    /// key value, None means key has been deleted
    /// but watch is not cancelled yet.
    kv: Option<KeyValue>,
}

impl CacheEntry {
    /// Create a new `CacheEntry`.
    pub const fn new(kv: Option<KeyValue>, revision: i64, watch_id: i64) -> Self {
        Self {
            watch_id,
            revision,
            kv,
        }
    }
}
/// Cache struct.
pub struct Cache {
    /// map to store `CacheEntry`
    hashtable: HashMap<Vec<u8>, CacheEntry>,
    /// lru queue of the keys in hashtable.
    lru_queue: PriorityQueue<Vec<u8>, u64>,
}

impl Cache {
    /// Create a new `Cache` with specified size.
    pub fn new(size: usize) -> Self {
        Self {
            hashtable: HashMap::with_capacity(size),
            lru_queue: PriorityQueue::new(),
        }
    }

    /// Search a `key` from the cache.
    pub fn search(&mut self, key: Vec<u8>) -> Option<KeyValue> {
        let search_result = self.hashtable.get(&key).cloned();
        match search_result {
            Some(entry) => entry.kv.map(|kv| {
                self.lru_queue.push(key, Self::get_priority());
                kv
            }),
            None => None,
        }
    }

    /// Check if a `key` is in cache
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.hashtable.contains_key(key)
    }

    /// Update a `key` to cache
    pub fn update(&mut self, key: &[u8], value: KeyValue, mark_delete: bool) {
        let entry = self
            .hashtable
            .get_mut(key)
            .unwrap_or_else(|| panic!("Key {:?} doesn't exist", key));
        let revision = value.get_mod_revision();
        if revision > entry.revision {
            if mark_delete {
                entry.kv = None
            } else {
                entry.kv = Some(value);
            }
            entry.revision = revision;
            self.lru_queue.push(key.to_vec(), Self::get_priority());
        }
    }

    /// Insert or update a `key` to the cache.
    pub fn insert_or_update(&mut self, key: Vec<u8>, value: KeyValue) {
        let revision = value.get_mod_revision();
        if self.hashtable.contains_key(&key) {
            self.update(&key, value, false);
        } else {
            self.hashtable
                .insert(key.clone(), CacheEntry::new(Some(value), revision, -1));
        }
        self.lru_queue.push(key, Self::get_priority());
    }

    /// Remove a `key` from cache totally
    pub fn remove(&mut self, key: &[u8]) {
        self.hashtable.remove(key);
        self.lru_queue.remove(key);
    }

    /// Mark a `key` as delete.
    pub fn mark_delete(&mut self, key: &[u8], value: KeyValue) {
        self.update(key, value, true);
    }

    /// Update watch id of a `key`
    pub fn update_watch_id(&mut self, key: Vec<u8>, watch_id: i64) {
        let entry = self
            .hashtable
            .get_mut(&key)
            .unwrap_or_else(|| panic!("Key {:?} doesn't exist", key));
        entry.watch_id = watch_id;
        self.lru_queue.push(key, Self::get_priority());
    }

    /// Gets the priority of a key in lru queue.
    fn get_priority() -> u64 {
        let current_time = SystemTime::now();
        let since_the_epoch = current_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("Fail to get the time since the epoch, the error is {}", e));

        u64::MAX.overflow_sub(since_the_epoch.as_secs())
    }

    /// Adjusts cache size if the number of value in cache has exceed the threshold(0.8 * capacity).
    /// Adjusts cache to 0.6 * capacity
    pub async fn adjust_cache_size(&mut self, watch_sender: &Sender<EtcdWatchRequest>) -> Res<()> {
        if self.hashtable.len() > self.hashtable.capacity().overflow_mul(8).overflow_div(10) {
            while self.hashtable.len() > self.hashtable.capacity().overflow_mul(6).overflow_div(10)
            {
                if let Some(pop_value) = self.lru_queue.pop() {
                    let key = pop_value.0;

                    if let Some(entry) = self.hashtable.remove(&key) {
                        watch_sender
                            .send(EtcdWatchRequest::cancel(entry.watch_id.cast()))
                            .await?;
                    } else {
                        panic!("lru cache doesn't match hashmap key is {:?}", key);
                    }
                } else {
                    panic!("lru cache size is less than hashmap size");
                }
            }
        }
        Ok(())
    }

    /// Clean cache
    pub fn clean(&mut self) {
        self.hashtable.clear();
        self.lru_queue.clear();
    }
}
