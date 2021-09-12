//! The implementation for Etcd cache.
use super::KeyValue;
use super::OverflowArithmetic;
use crate::watch::EtcdWatchRequest;
use crate::Result as Res;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use priority_queue::PriorityQueue;
use smol::channel::Sender;
use smol::lock::Mutex;
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
/// Cache struct contains a lock-free hashTable.
pub struct Cache {
    /// map to store key value
    hashtable: LockFreeCuckooHash<Vec<u8>, CacheEntry>,
    /// lru queue to store the keys in hashtable.
    lru_queue: Mutex<PriorityQueue<Vec<u8>, u64>>,
}

impl Cache {
    /// Create a new `Cache` with specified size.
    pub fn new(size: usize) -> Self {
        Self {
            hashtable: LockFreeCuckooHash::with_capacity(size),
            lru_queue: Mutex::new(PriorityQueue::new()),
        }
    }

    /// Searches a `key` from the cache.
    pub async fn search(&self, key: Vec<u8>) -> Option<KeyValue> {
        let search_result = {
            let guard = pin();
            self.hashtable.get(&key, &guard).cloned()
        };
        match search_result {
            Some(entry) => {
                if let Some(kv) = entry.kv {
                    self.lru_queue.lock().await.push(key, Self::get_priority());
                    Some(kv)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Update a `key` to cache
    pub async fn update(&self, key: Vec<u8>, value: KeyValue, mark_delete: bool) {
        loop {
            let value_clone = value.clone();
            let search_result = {
                let guard = pin();
                self.hashtable.get(&key, &guard).cloned()
            };
            if let Some(ref entry) = search_result {
                let revision = value_clone.get_mod_revision();
                if revision > entry.revision {
                    if self.hashtable.compare_and_update(
                        key.clone(),
                        CacheEntry::new(
                            if mark_delete { None } else { Some(value_clone) },
                            revision,
                            entry.watch_id,
                        ),
                        entry,
                    ) {
                        self.lru_queue
                            .lock()
                            .await
                            .push(key.clone(), Self::get_priority());
                        break;
                    } else {
                        continue;
                    }
                } else {
                    break;
                }
            } else {
                panic!("key {:?} is deleted during update", key);
            }
        }
    }

    /// Insert or update a `key` to the cache.
    pub async fn insert_or_update(&self, key: Vec<u8>, value: KeyValue) {
        let revision = value.get_mod_revision();
        if self.hashtable.insert_if_not_exists(
            key.clone(),
            CacheEntry::new(Some(value.clone()), revision, -1),
        ) {
            self.lru_queue
                .lock()
                .await
                .push(key.clone(), Self::get_priority());
        } else {
            self.update(key, value, false).await;
        }
    }

    /// Remove a `key` from cache totally
    pub async fn remove(&self, key: &[u8]) {
        self.hashtable.remove(key);
        self.lru_queue.lock().await.remove(key);
    }

    /// Mark a `key` as delete.
    pub async fn mark_delete(&self, key: Vec<u8>, value: KeyValue) {
        let search_result = {
            let guard = pin();
            self.hashtable.get(&key, &guard).cloned()
        };
        if search_result.is_some() {
            self.update(key, value, true).await;
        }
    }

    /// Update watch id of a `key`
    pub async fn update_watch_id(&self, key: Vec<u8>, watch_id: i64) {
        loop {
            let search_result = {
                let guard = pin();
                self.hashtable.get(&key, &guard).cloned()
            };
            if let Some(ref entry) = search_result {
                if self.hashtable.compare_and_update(
                    key.clone(),
                    CacheEntry::new(entry.kv.clone(), entry.revision, watch_id),
                    entry,
                ) {
                    self.lru_queue
                        .lock()
                        .await
                        .push(key.clone(), Self::get_priority());
                    break;
                } else {
                    continue;
                }
            } else {
                panic!("key {:?} is deleted during update", key);
            }
        }
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
    pub async fn adjust_cache_size(&self, watch_sender: &Sender<EtcdWatchRequest>) -> Res<()> {
        if self.hashtable.size() > self.hashtable.capacity().overflow_mul(8).overflow_div(10) {
            let mut queue = self.lru_queue.lock().await;
            while self.hashtable.size() > self.hashtable.capacity().overflow_mul(6).overflow_div(10)
            {
                if let Some(pop_value) = queue.pop() {
                    let key = pop_value.0;
                    let search_result = {
                        let guard = pin();
                        self.hashtable.get(&key, &guard).cloned()
                    };
                    if let Some(value) = search_result {
                        self.hashtable.remove(&key);
                        watch_sender
                            .send(EtcdWatchRequest::cancel(value.watch_id.cast()))
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
    #[allow(unsafe_code)]
    pub async fn clean(&self) {
        unsafe {
            self.hashtable.clear();
        }
        let mut queue = self.lru_queue.lock().await;
        queue.clear();
    }
}
