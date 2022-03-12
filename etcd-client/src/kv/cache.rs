//! The implementation for Etcd cache.
use super::KeyValue;
use super::OverflowArithmetic;
use super::WatchRequest;
use crate::Result as Res;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use priority_queue::PriorityQueue;
use smol::channel::Sender;
use smol::lock::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Cache entry
#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    /// current revision of key in cache
    revision: i64,
    /// key value, None means key has been deleted
    /// but watch is not cancelled yet.
    kv: Option<KeyValue>,
}

impl CacheEntry {
    /// Create a new `CacheEntry`.
    pub const fn new(kv: Option<KeyValue>, revision: i64) -> Self {
        Self { revision, kv }
    }
}
/// Cache struct contains a lock-free hashTable.
pub struct Cache {
    /// map to store key value
    hashtable: LockFreeCuckooHash<Vec<u8>, CacheEntry>,
    /// lru queue of the keys in hashtable.
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
    pub async fn search(&self, key: &[u8]) -> Option<KeyValue> {
        let search_result = {
            let guard = pin();
            self.hashtable.get(key, &guard).cloned()
        };
        match search_result {
            Some(entry) => {
                if let Some(kv) = entry.kv {
                    self.lru_queue
                        .lock()
                        .await
                        .change_priority(key, Self::get_priority());
                    Some(kv)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Check if the new `CacheEntry` has higher revision.
    const fn higher_revision(old: &CacheEntry, new: &CacheEntry) -> bool {
        new.revision > old.revision
    }

    /// Helper function to insert or update a `key` to the cache.
    /// Return `(bool, bool)` first bool indicates if operation succeed
    /// second bool indicates if it is an insert or not.
    async fn insert_or_update_helper(
        &self,
        key: Vec<u8>,
        value: KeyValue,
        mark_delete: bool,
    ) -> (bool, bool) {
        let revision = value.get_mod_revision();
        let res = {
            let guard = &pin();
            let (succeed, old_value) = self.hashtable.insert_or_update_on(
                key.clone(),
                CacheEntry::new(if mark_delete { None } else { Some(value) }, revision),
                Self::higher_revision,
                guard,
            );
            (succeed, old_value.is_none())
        };
        self.lru_queue.lock().await.push(key, Self::get_priority());
        res
    }

    /// Insert or update a `key` to the cache.
    pub async fn insert_or_update(&self, key: Vec<u8>, value: KeyValue) -> (bool, bool) {
        self.insert_or_update_helper(key, value, false).await
    }

    /// Remove a `key` from cache totally
    pub async fn remove(&self, key: &[u8]) {
        self.lru_queue.lock().await.remove(key);
        self.hashtable.remove(key);
    }

    /// Mark a `key` as delete.
    pub async fn mark_delete(&self, key: Vec<u8>, value: KeyValue) {
        self.insert_or_update_helper(key, value, true).await;
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
    pub async fn adjust_cache_size(&self, watch_sender: &Sender<WatchRequest>) -> Res<()> {
        if let Some(mut queue) = self.lru_queue.try_lock() {
            let upper_bound = self.hashtable.capacity().overflow_mul(8).overflow_div(10);
            let lower_bound = self.hashtable.capacity().overflow_mul(6).overflow_div(10);

            if queue.len() > upper_bound {
                while queue.len() > lower_bound {
                    if let Some(pop_value) = queue.pop() {
                        let key = pop_value.0;
                        watch_sender.send(WatchRequest::cancel(key)).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
