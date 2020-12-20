//! The implementation for Etcd cache.
use super::KeyValue;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::sync::Arc;

/// Cache struct contains a lock-free hashTable.
#[derive(Clone)]
pub struct Cache {
    /// map to store key value
    hashtable: Arc<LockFreeCuckooHash<Vec<u8>, KeyValue>>,
}

impl Cache {
    /// Create a new `Cache` with specified size.
    pub fn new(size: usize) -> Self {
        Self {
            hashtable: Arc::new(LockFreeCuckooHash::with_capacity(size)),
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

    /// Inserts a `key` from the cache.
    pub fn insert(&self, key: Vec<u8>, value: KeyValue) {
        self.hashtable.insert(key, value);
    }

    /// Deletes a `key` from the cache.
    pub fn delete(&self, key: &[u8]) {
        self.hashtable.remove(&key.to_vec());
    }
}
