package kvstore

import (
	"sync"
	"time"

	"github.com/axaxs/semaphore"
)

var (
	// Generally, to increase insertions at the speed of reaping
	// you'd want to increate NumStores and/or decrease NumReapers

	// Alternatively, if you'd rather get your reaping time down
	// consider setting them both lower and perhaps equal to each other

	// NumStores is how many buckets to divide between
	NumStores uint32 = 64
	// NumReapers is how many concurrent buckets to reap at a time
	NumReapers uint8 = 4
)

// KVStore is our primary key-value store.  It is a parent container/controller used for all operations
// Do not use the empty value of this struct, it will panic.  Use NewKVStore()
type KVStore struct {
	sema      *semaphore.Semaphore
	reapLock  sync.Mutex
	stores    map[uint32]*substore
	numStores uint32
}

// NewKVStore inits the internal map and returns a KVStore
func NewKVStore() KVStore {
	kvs := KVStore{sema: semaphore.NewSemaphore(int(NumReapers)), numStores: NumStores, stores: make(map[uint32]*substore)}
	for i := uint32(0); i < kvs.numStores; i++ {
		nm := &substore{kvmap: map[string]item{}, num: i}
		kvs.stores[i] = nm
	}

	return kvs
}

func (kvs *KVStore) getStore(num uint32) *substore {
	return kvs.stores[num]
}

// Set sets the given key to value.  If ttl is 0, it never expires.  Otherwise, it expires at insertion time + duration
func (kvs *KVStore) Set(key string, value interface{}, ttl time.Duration) {
	storeNum := qhash([]byte(key), kvs.numStores)

	kvs.getStore(storeNum).set(key, value, ttl)
}

// SetNX sets only if key doesn't exist.  It returns whether or not the operation was successful.
// In other words, if the key existed, you get back false
func (kvs *KVStore) SetNX(key string, value interface{}, ttl time.Duration) bool {
	storeNum := qhash([]byte(key), kvs.numStores)

	return kvs.getStore(storeNum).setNX(key, value, ttl)
}

// Get gets the value stored at key, and whether or not it existed
// Always check the existed value, as the return value will always be nil if it's false
func (kvs *KVStore) Get(key string) (interface{}, bool) {
	storeNum := qhash([]byte(key), kvs.numStores)

	return kvs.getStore(storeNum).get(key)
}

// Delete deletes the key from the store
func (kvs *KVStore) Delete(key string) {
	u := qhash([]byte(key), kvs.numStores)

	kvs.getStore(u).delete(key)
}

// Reap stops the world to scan the entire map and evicts expired things.
// results is whether or not to collect deleted keys for return
// If results is false, the returned map will always be nil
// It does not run automatically, and is meant to be user implementable.
func (kvs *KVStore) Reap(results bool) map[string]interface{} {
	kvs.reapLock.Lock()
	defer kvs.reapLock.Unlock()

	var totalDeleted map[string]interface{}
	if results {
		totalDeleted = make(map[string]interface{})
	}

	var reapMut sync.Mutex

	for _, store := range kvs.stores {
		kvs.sema.Grab()
		go func(s *substore) {
			defer kvs.sema.Release()
			deleted := s.reap(results)
			if results {
				reapMut.Lock()
				for k, v := range deleted {
					totalDeleted[k] = v
				}
				reapMut.Unlock()
			}
		}(store)
	}

	kvs.sema.Wait()

	return totalDeleted
}

// Keys returns all of the keys in a slice
// This is used for iterating, as one can use a Get on each key
func (kvs *KVStore) Keys() []string {
	ret := make([]string, 0, kvs.Len())
	for _, store := range kvs.stores {
		ret = append(ret, store.keys()...)
	}

	return ret
}

// Len returns a rough approximation of how many items total are in the kvstore
func (kvs *KVStore) Len() int {
	l := 0
	for _, store := range kvs.stores {
		l += store.len()
	}

	return l
}
