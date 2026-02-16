package ledis

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestAOFPersistence(t *testing.T) {
	// Cleanup
	os.Remove("appendonly.aof")
	defer os.Remove("appendonly.aof")

	// 1. Start DB, Write Data
	db := New(4)
	db.Set("k1", "v1", 0)
	db.LogCommand("SET", "k1", "v1") // Manual logging for library test

	db.Set("k2", "v2", 0)
	db.LogCommand("SET", "k2", "v2")

	db.RPush("list1", "a", "b", "c")
	db.LogCommand("RPUSH", "list1", "a", "b", "c")

	// Wait for background write (channel)
	time.Sleep(200 * time.Millisecond)
	db.Close()

	// 2. Restart DB (Recovery)
	db2 := New(4)

	v, _ := db2.Get("k1")
	if v == nil || v.Str != "v1" {
		t.Errorf("Expected k1=v1, got %v", v)
	}

	l, _ := db2.LLen("list1")
	if l != 3 {
		t.Errorf("Expected list1 len 3, got %d", l)
	}

	db2.Close()
}

func TestAOFRewrite(t *testing.T) {
	os.Remove("appendonly.aof")
	defer os.Remove("appendonly.aof")

	db := New(4)
	// Write lots of data
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i)
		db.Set(k, "v", 0)
		db.LogCommand("SET", k, "v")
	}

	time.Sleep(100 * time.Millisecond)

	// Rewrite
	err := db.RewriteAOF()
	if err != nil {
		t.Fatalf("RewriteAOF failed: %v", err)
	}

	// Verify file exists and has content (we don't parse it here, assume LoadAOF works)
	info, err := os.Stat("appendonly.aof")
	if err != nil {
		t.Fatalf("AOF file missing after rewrite")
	}
	if info.Size() == 0 {
		t.Errorf("AOF file is empty after rewrite")
	}

	db.Close()

	// Recover from Rewritten AOF
	db2 := New(4)

	// Check random keys
	v, _ := db2.Get("k0")
	if v == nil || v.Str != "v" {
		t.Errorf("Recovered k0 missing or wrong")
	}
	v, _ = db2.Get("k99")
	if v == nil || v.Str != "v" {
		t.Errorf("Recovered k99 missing or wrong")
	}

	db2.Close()
}
