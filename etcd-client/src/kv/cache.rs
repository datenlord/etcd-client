//! The implementation for Etcd cache.
use super::KeyValue;
use super::OverflowArithmetic;
use crate::watch::EtcdWatchRequest;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use priority_queue::PriorityQueue;
use smol::channel::Sender;
use smol::lock::Mutex;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use utilities::Cast;

/// Cache struct contains a lock-free hashTable.
#[derive(Clone)]
pub struct Cache {
    /// map to store key value
    hashtable: Arc<LockFreeCuckooHash<Vec<u8>, KeyValue>>,
    /// lru queue to store the keys in hashtable.
    lru_queue: Arc<Mutex<PriorityQueue<Vec<u8>, u64>>>,
    /// map to store key and watch id
    watch_id_table: Arc<LockFreeCuckooHash<Vec<u8>, i64>>,
    /// Etcd watch request sender.
    watch_sender: Sender<EtcdWatchRequest>,
}

impl Cache {
    /// Create a new `Cache` with specified size.
    pub fn new(size: usize, sender: Sender<EtcdWatchRequest>) -> Self {
        Self {
            hashtable: Arc::new(LockFreeCuckooHash::with_capacity(size)),
            lru_queue: Arc::new(Mutex::new(PriorityQueue::new())),
            watch_id_table: Arc::new(LockFreeCuckooHash::with_capacity(size)),
            watch_sender: sender,
        }
    }

    /// Searches a `key` from the cache.
    pub async fn search(&self, key: Vec<u8>) -> Option<KeyValue> {
        let search_result = {
            let guard = pin();
            self.hashtable.get(&key, &guard).cloned()
        };
        match search_result {
            Some(value) => {
                self.lru_queue.lock().await.push(key, Self::get_priority());
                Some(value.clone())
            }
            None => None,
        }
    }

    /// Inserts a `key` from the cache.
    pub async fn insert(&self, key: Vec<u8>, value: KeyValue) {
        self.hashtable.insert(key.clone(), value);
        self.lru_queue.lock().await.push(key, Self::get_priority());
        self.adjust_cache_size().await;
    }

    /// Deletes a `key` from the cache.
    pub async fn delete(&self, key: Vec<u8>, cancel_watch: bool) {
        if cancel_watch {
            let watch_id = self
                .search_watch_id(&key.clone())
                .unwrap_or_else(|| panic!("Fail to get watch id for a key"));
            // Remove watch for this key.
            self.watch_sender
                .send(EtcdWatchRequest::cancel(watch_id.cast()))
                .await
                .unwrap_or_else(|e| panic!("Fail to send watch request, the error is {}", e));
        } else {
            self.delete_watch_id(&key);
        }

        self.hashtable.remove(&key);
        self.lru_queue.lock().await.remove(&key);
    }

    /// Deletes a watch id from the cache.
    pub fn delete_watch_id(&self, key: &[u8]) {
        self.watch_id_table.remove(&key.to_vec());
    }

    /// Inserts a watch id to the cache.
    pub fn insert_watch_id(&self, key: Vec<u8>, value: i64) {
        self.watch_id_table.insert(key, value);
    }

    /// Searches a watch id for a specific key.
    pub fn search_watch_id(&self, key: &[u8]) -> Option<i64> {
        let guard = pin();
        let search_result = self.watch_id_table.get(&key.to_vec(), &guard);
        match search_result {
            Some(value) => Some(*value),
            None => None,
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

    /// Adjusts cache size if the number of value in cache has exceed the threshold(0.6).
    async fn adjust_cache_size(&self) {
        if self.hashtable.size() > self.hashtable.capacity().overflow_mul(6).overflow_div(10) {
            let queue = self.lru_queue.lock().await;
            if let Some(pop_value) = queue.peek() {
                self.delete(pop_value.0.to_vec().clone(), false).await;
            }
        }
    }

    /// Clean cache
    #[allow(unsafe_code)]
    pub async fn clean(&self) {
        unsafe {
            self.hashtable.clear();
            self.watch_id_table.clear();
        }
        let mut queue = self.lru_queue.lock().await;
        queue.clear();
    }
}
