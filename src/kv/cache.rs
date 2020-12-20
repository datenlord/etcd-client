//! The implementation for Etcd cache.
use super::KeyValue;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::sync::Arc;

/// Cache struct contains a lock-free hashTable.
#[derive(Clone)]
pub struct Cache {
    /// map to store key value
    hashtable: Arc<LockFreeCuckooHash<Vec<u8>, KeyValue>>,
    /// map to store key and watch id
    watch_id_table: Arc<LockFreeCuckooHash<Vec<u8>, i64>>,
}

impl Cache {
    /// Create a new `Cache` with specified size.
    pub fn new(size: usize) -> Self {
        Self {
            hashtable: Arc::new(LockFreeCuckooHash::with_capacity(size)),
            watch_id_table: Arc::new(LockFreeCuckooHash::with_capacity(size)),
        }
    }

    /// Searches a `key` from the cache.
    pub fn search(&self, key: &[u8]) -> Option<KeyValue> {
        let guard = pin();
        let search_result = self.hashtable.search_with_guard(&key.to_vec(), &guard);
        match search_result {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    /// Inserts a `key` to the cache.
    pub fn insert(&self, key: Vec<u8>, value: KeyValue) {
        self.hashtable.insert(key, value);
    }

    /// Deletes a `key` from the cache.
    pub fn delete(&self, key: &[u8]) {
        self.hashtable.remove(&key.to_vec());
    }

    /// Inserts a watch id to the cache.
    pub fn insert_watch_id(&self, key: Vec<u8>, value: i64) {
        self.watch_id_table.insert(key, value);
    }

    /// Searches a watch id for a specific key.
    pub fn search_watch_id(&self, key: &[u8]) -> Option<i64> {
        let guard = pin();
        let search_result = self.watch_id_table.search_with_guard(&key.to_vec(), &guard);
        match search_result {
            Some(value) => Some(*value),
            None => None,
        }
    }
}

/// Cache value struct.
#[derive(Clone, PartialEq)]
pub struct CacheValue {
    /// Etcd `KeyValue` pairs struct.
    key_value: KeyValue,
}
