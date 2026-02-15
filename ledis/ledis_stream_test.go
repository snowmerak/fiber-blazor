package ledis

import (
	"testing"
)

func TestStreamBasics(t *testing.T) {
	db := New(16)
	key := "mystream"

	// XADD
	id1, err := db.XAdd(key, "*", 0, "sensor-id", "1234", "temp", "19.8")
	if err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}
	if id1 == "" {
		t.Fatal("XAdd returned empty ID")
	}

	id2, err := db.XAdd(key, "*", 0, "sensor-id", "1234", "temp", "20.4")
	if err != nil {
		t.Fatalf("XAdd 2 failed: %v", err)
	}

	// XLEN
	l, err := db.XLen(key)
	if l != 2 {
		t.Errorf("XLen expected 2, got %d", l)
	}

	// XRANGE
	entries, err := db.XRange(key, "-", "+")
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("XRange expected 2 entries, got %d", len(entries))
	}
	if entries[0].ID != id1 {
		t.Errorf("First entry ID expected %s, got %s", id1, entries[0].ID)
	}

	// XRANGE with start/end
	entries, _ = db.XRange(key, id1, id1) // inclusive
	if len(entries) != 1 {
		t.Errorf("XRange specific expected 1, got %d", len(entries))
	}

	// XREVRANGE
	entries, err = db.XRevRange(key, "+", "-")
	if len(entries) != 2 {
		t.Errorf("XRevRange expected 2, got %d", len(entries))
	}
	if entries[0].ID != id2 {
		t.Errorf("XRevRange first expected %s, got %s", id2, entries[0].ID)
	}
}

func TestStreamIDValidation(t *testing.T) {
	db := New(16)
	key := "stream_id_test"

	// 100-1
	_, err := db.XAdd(key, "100-1", 0, "f", "v")
	if err != nil {
		t.Fatalf("XAdd 100-1 failed: %v", err)
	}

	// 100-1 duplicate (should fail)
	_, err = db.XAdd(key, "100-1", 0, "f", "v")
	if err == nil {
		t.Error("XAdd duplicate 100-1 should fail")
	}

	// 100-0 smaller (should fail)
	_, err = db.XAdd(key, "100-0", 0, "f", "v")
	if err == nil {
		t.Error("XAdd smaller 100-0 should fail")
	}

	// 0-0 (should fail)
	db.Del(key)
	_, err = db.XAdd(key, "0-0", 0, "f", "v")
	if err == nil {
		t.Error("XAdd 0-0 should fail")
	}
}

func TestXRead(t *testing.T) {
	db := New(16)
	k1 := "s1"
	k2 := "s2"

	id1_1, _ := db.XAdd(k1, "*", 0, "k", "v1")
	id1_2, _ := db.XAdd(k1, "*", 0, "k", "v2")

	_, _ = db.XAdd(k2, "*", 0, "k", "v3")

	// XREAD s1 from 0, s2 from 0
	streams := map[string]string{
		k1: "0-0",
		k2: "0-0",
	}

	res, err := db.XRead(streams, 0)
	if err != nil {
		t.Fatalf("XRead failed: %v", err)
	}

	if len(res[k1]) != 2 {
		t.Errorf("s1 count expected 2, got %d", len(res[k1]))
	}
	if len(res[k2]) != 1 {
		t.Errorf("s2 count expected 1, got %d", len(res[k2]))
	}

	// XREAD s1 from id1_1 (should get id1_2)
	streams[k1] = id1_1
	res, _ = db.XRead(streams, 0)

	if len(res[k1]) != 1 {
		t.Errorf("s1 count expected 1, got %d", len(res[k1]))
	}
	if res[k1][0].ID != id1_2 {
		t.Errorf("Expected s1 msg to be v2")
	}

	// Count limit
	streams[k1] = "0-0"
	res, _ = db.XRead(streams, 1) // count 1
	if len(res[k1]) != 1 {
		t.Errorf("s1 count expected 1 due to limit, got %d", len(res[k1]))
	}
}
