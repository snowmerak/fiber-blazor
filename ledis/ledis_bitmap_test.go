package ledis

import (
	"testing"
)

func TestBitmapBasics(t *testing.T) {
	db := New(16)
	key := "bmp"

	// SETBIT
	old, err := db.SetBit(key, 10, 1)
	if err != nil {
		t.Fatalf("SetBit failed: %v", err)
	}
	if old != 0 {
		t.Errorf("Expected old 0, got %d", old)
	}

	old, err = db.SetBit(key, 10, 0) // Clear
	if old != 1 {
		t.Errorf("Expected old 1, got %d", old)
	}

	db.SetBit(key, 10, 1)
	db.SetBit(key, 20, 1)

	// GETBIT
	val, err := db.GetBit(key, 10)
	if val != 1 {
		t.Errorf("GetBit 10 expected 1, got %d", val)
	}
	val, _ = db.GetBit(key, 15) // unset
	if val != 0 {
		t.Errorf("GetBit 15 expected 0, got %d", val)
	}

	// BITCOUNT
	cnt, err := db.BitCount(key)
	if cnt != 2 {
		t.Errorf("BitCount expected 2, got %d", cnt)
	}
}

func TestBitOp(t *testing.T) {
	db := New(16)
	k1 := "b1"
	k2 := "b2"
	dest := "bout"

	// k1: {1, 2, 3}
	db.SetBit(k1, 1, 1)
	db.SetBit(k1, 2, 1)
	db.SetBit(k1, 3, 1)

	// k2: {2, 3, 4}
	db.SetBit(k2, 2, 1)
	db.SetBit(k2, 3, 1)
	db.SetBit(k2, 4, 1)

	// AND -> {2, 3} (count 2)
	cnt, err := db.BitOp("AND", dest, k1, k2)
	if err != nil {
		t.Fatalf("BitOp AND failed: %v", err)
	}
	if cnt != 2 {
		t.Errorf("BitOp AND count expected 2, got %d", cnt)
	}
	v, _ := db.GetBit(dest, 1)
	if v != 0 {
		t.Errorf("BitOp AND bit 1 should be 0")
	}
	v, _ = db.GetBit(dest, 2)
	if v != 1 {
		t.Errorf("BitOp AND bit 2 should be 1")
	}

	// OR -> {1, 2, 3, 4} (count 4)
	cnt, _ = db.BitOp("OR", dest, k1, k2)
	if cnt != 4 {
		t.Errorf("BitOp OR count expected 4, got %d", cnt)
	}

	// XOR -> {1, 4} (count 2)
	cnt, _ = db.BitOp("XOR", dest, k1, k2)
	if cnt != 2 {
		t.Errorf("BitOp XOR count expected 2, got %d", cnt)
	}
	v, _ = db.GetBit(dest, 1)
	if v != 1 {
		t.Errorf("BitOp XOR bit 1 should be 1")
	}
	v, _ = db.GetBit(dest, 2)
	if v != 0 {
		t.Errorf("BitOp XOR bit 2 should be 0")
	}

	// NOT (k1: {1,2,3}, Max 3 -> Flip 0..3 -> {0})?
	// Logic: Flip(0, 4) -> 0..3
	// {1,2,3} -> {0}
	cnt, _ = db.BitOp("NOT", dest, k1)
	// If NOT behavior is Flip(0, Max+1), then for {1,2,3}:
	// 0: 0->1
	// 1: 1->0
	// 2: 1->0
	// 3: 1->0
	// Result: {0}
	if cnt != 1 {
		t.Errorf("BitOp NOT count expected 1, got %d", cnt)
	}
	v, _ = db.GetBit(dest, 0)
	if v != 1 {
		t.Errorf("BitOp NOT bit 0 should be 1")
	}
}
