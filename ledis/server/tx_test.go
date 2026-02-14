package server

import (
	"net"
	"testing"

	"github.com/snowmerak/fiber-blazor/ledis"
)

func mustRead(t *testing.T, r *Reader, expectedType byte, expectedStr string) *Value {
	val, err := r.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if val.Type != expectedType {
		t.Fatalf("Expected type %c, got %c", expectedType, val.Type)
	}
	if expectedStr != "" {
		switch expectedType {
		case SimpleString, Error:
			if val.Str != expectedStr {
				t.Fatalf("Expected %s, got %s", expectedStr, val.Str)
			}
		case BulkString:
			if val.Bulk != expectedStr {
				t.Fatalf("Expected %s, got %s", expectedStr, val.Bulk)
			}
		}
	}
	return &val
}

func TestTx_Simple(t *testing.T) {
	db := ledis.New(16)
	h := NewHandler(db)
	c, s := net.Pipe()
	go h.Handle(s)
	defer c.Close()
	r := NewReader(c)

	// MULTI
	c.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// SET k v
	c.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	mustRead(t, r, SimpleString, "QUEUED")

	// GET k
	c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	mustRead(t, r, SimpleString, "QUEUED")

	// EXEC
	c.Write([]byte("*1\r\n$4\r\nEXEC\r\n"))
	val, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array || len(val.Array) != 2 {
		t.Fatalf("Expected Array(2), got %v", val)
	}
	if val.Array[0].Str != "OK" {
		t.Fatalf("Expected OK, got %v", val.Array[0])
	}
	if val.Array[1].Bulk != "v" {
		t.Fatalf("Expected v, got %v", val.Array[1])
	}
}

func TestTx_Discard(t *testing.T) {
	db := ledis.New(16)
	h := NewHandler(db)
	c, s := net.Pipe()
	go h.Handle(s)
	defer c.Close()
	r := NewReader(c)

	c.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
	mustRead(t, r, SimpleString, "OK")

	c.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	mustRead(t, r, SimpleString, "QUEUED")

	c.Write([]byte("*1\r\n$7\r\nDISCARD\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// Verify k is not set
	c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	val, _ := r.Read()
	// Should be Null
	if !val.IsNull {
		t.Fatalf("Expected Null check for DISCARD, got %v", val)
	}
}

func TestTx_Watch_Success(t *testing.T) {
	db := ledis.New(16)
	h := NewHandler(db)
	c, s := net.Pipe()
	go h.Handle(s)
	defer c.Close()
	r := NewReader(c)

	// WATCH k
	c.Write([]byte("*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// MULTI
	c.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// SET k v
	c.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	mustRead(t, r, SimpleString, "QUEUED")

	// EXEC
	c.Write([]byte("*1\r\n$4\r\nEXEC\r\n"))
	val, _ := r.Read()
	if val.Type != Array {
		t.Fatalf("Expected Success (Array), got %v", val)
	}
}

func TestTx_Watch_Fail(t *testing.T) {
	db := ledis.New(16)
	h := NewHandler(db)
	c, s := net.Pipe()
	go h.Handle(s)
	defer c.Close()
	r := NewReader(c)

	// WATCH k
	c.Write([]byte("*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// Simulate external modification
	db.Set("k", "v2", 0)

	// MULTI
	c.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
	mustRead(t, r, SimpleString, "OK")

	// SET k v
	c.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	mustRead(t, r, SimpleString, "QUEUED")

	// EXEC -> Should fail (return Null)
	c.Write([]byte("*1\r\n$4\r\nEXEC\r\n"))
	val, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}

	if !val.IsNull {
		t.Fatalf("Expected Transaction Failure (Null), got: %v", val)
	}
}
