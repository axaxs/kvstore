package kvstore

import (
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	testKey := "test"
	nkvs := NewKVStore()
	nkvs.Set(testKey, 2, 0)

	x, ok := nkvs.Get(testKey)
	if !ok {
		t.Fatal("inserted key was not found upon retrieval")
	}

	res, ok := x.(int)
	if !ok {
		t.Fatal("value received back was not an integer")
	}

	if res != 2 {
		t.Fatalf("expected 2; got %d", res)
	}
}

func TestSetNX(t *testing.T) {
	testKey := "test"
	nkvs := NewKVStore()
	ok := nkvs.SetNX(testKey, 2, 0)
	if !ok {
		t.Fatal("first setnx should have returned true; got false")
	}

	ok = nkvs.SetNX(testKey+"3", 3, time.Second)
	if !ok {
		t.Fatal("first setnx should have returned true; got false")
	}

	ok = nkvs.SetNX(testKey, 4, 0)
	if ok {
		t.Fatal("second setnx should have returned false; got true")
	}

	x, ok := nkvs.Get(testKey)
	if !ok {
		t.Fatal("inserted key was not found upon retrieval")
	}

	res, ok := x.(int)
	if !ok {
		t.Fatal("value received back was not an integer")
	}

	if res != 2 {
		t.Fatalf("expected 2; got %d", res)
	}

}

func TestTTL(t *testing.T) {
	testKey := "test"
	nkvs := NewKVStore()
	nkvs.Set(testKey, 2, 1*time.Second)
	nkvs.Set(testKey+"3", 3, 0)

	if nkvs.Len() != 2 {
		t.Fatalf("expected store length 2; got length %d", nkvs.Len())
	}

	time.Sleep(2 * time.Second)
	deleted := nkvs.Reap(true)
	if len(deleted) != 1 {
		t.Fatalf("expected 1 deleted; got %d", len(deleted))
	}

	if nkvs.Len() != 1 {
		t.Fatalf("expected store length 1; got length %d", nkvs.Len())
	}

	_, ok := nkvs.Get(testKey)
	if ok {
		t.Fatal("expected reaped testKey to be gone; got ok")
	}
}

func TestDelete(t *testing.T) {
	testKey := "test"
	nkvs := NewKVStore()
	nkvs.Set(testKey, 2, 0)
	nkvs.Delete(testKey)
	_, ok := nkvs.Get(testKey)
	if ok {
		t.Fatal("expected reaped testKey to be gone; got ok")
	}

}

func TestKeys(t *testing.T) {
	testKey := "test"
	nkvs := NewKVStore()
	nkvs.Set(testKey, 2, 0)
	keys := nkvs.Keys()
	if len(keys) != 1 || keys[0] != testKey {
		t.Fatal("expected 1 item, test")
	}

}
