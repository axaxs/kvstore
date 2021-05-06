package kvstore

import (
	"sync"
	"time"
)

// an item just represents an interface with an expiration tied to it
type item struct {
	val    interface{}
	expiry time.Time
}

// a substore contains a val representing its store number, and the maps themselves
type substore struct {
	sync.RWMutex
	num   uint32
	kvmap map[string]item
}

func (store *substore) set(key string, value interface{}, ttl time.Duration) {
	store.Lock()
	defer store.Unlock()

	i := item{val: value}
	if ttl > 0 {
		i.expiry = getTimeCoarse().Add(ttl)
	}

	store.kvmap[key] = i
}

func (store *substore) setNX(key string, value interface{}, ttl time.Duration) bool {
	store.Lock()
	defer store.Unlock()

	_, ok := store.kvmap[key]
	if ok {
		return false
	}

	i := item{val: value}
	if ttl > 0 {
		i.expiry = getTimeCoarse().Add(ttl)
	}

	store.kvmap[key] = i

	return true
}

func (store *substore) get(key string) (interface{}, bool) {
	store.RLock()
	defer store.RUnlock()

	i, ok := store.kvmap[key]
	if !ok {
		return nil, false
	}

	return i.val, true
}

func (store *substore) delete(key string) {
	store.Lock()
	defer store.Unlock()

	delete(store.kvmap, key)
}

func (store *substore) reap(results bool) map[string]interface{} {
	resMap := make(map[string]interface{})
	now := getTimeCoarse()

	store.Lock()
	defer store.Unlock()

	for k, v := range store.kvmap {
		if v.expiry.IsZero() || v.expiry.After(now) {
			continue
		}

		delete(store.kvmap, k)
		if results {
			resMap[k] = v.val
		}
	}

	return resMap
}

func (store *substore) keys() []string {
	store.RLock()
	defer store.RUnlock()

	keys := make([]string, 0, len(store.kvmap))
	for k := range store.kvmap {
		keys = append(keys, k)
	}

	return keys
}

func (store *substore) len() int {
	store.RLock()
	defer store.RUnlock()
	return len(store.kvmap)
}
