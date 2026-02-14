package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/snowmerak/fiber-blazor/ledis"
)

func TestSCC_Manual(t *testing.T) {
	db := ledis.New(16)
	handler := NewHandler(db)

	c1, s1 := net.Pipe()
	go handler.Handle(s1)
	defer c1.Close()

	reader := NewReader(c1)

	// 1. Send CLIENT TRACKING ON
	c1.Write([]byte("*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n"))
	val, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != SimpleString || val.Str != "OK" {
		t.Fatalf("Expected OK, got %v", val)
	}

	// 2. Get k1 (Subscribes to k1)
	c1.Write([]byte("*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"))
	reader.Read() // Read NULL response

	// 3. Update k1 from another connection
	db.Set("k1", "v2", 0)

	// 4. Expect Push message on c1
	pushVal, err := reader.Read()
	if err != nil {
		t.Fatalf("Expected Push message, got error: %v", err)
	}
	if pushVal.Type != Push {
		t.Fatalf("Expected Push type '>', got %c", pushVal.Type)
	}
	// Content: ["invalidate", ["k1"]]
	if len(pushVal.Array) != 2 || pushVal.Array[0].Bulk != "invalidate" {
		t.Fatalf("Unexpected Push content: %v", pushVal)
	}
	keys := pushVal.Array[1].Array
	if len(keys) != 1 || keys[0].Bulk != "k1" {
		t.Fatalf("Expected key k1 in invalidation, got %v", keys)
	}
}

func TestSCC_GoRedis_RESP3(t *testing.T) {
	db := ledis.New(16)
	handler := NewHandler(db)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler.Handle(conn)
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Protocol: 3,
	})
	defer rdb.Close()
	ctx := context.Background()

	// 1. Set key
	err = rdb.Set(ctx, "k1", "v1", 0).Err()
	if err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	// 2. Get key
	val, err := rdb.Get(ctx, "k1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Fatalf("Expected v1, got %s", val)
	}
}

func TestSCC_MultiClient(t *testing.T) {
	db := ledis.New(16)
	handler := NewHandler(db)

	createTrackingClient := func() (net.Conn, *Reader) {
		c1, s1 := net.Pipe()
		go handler.Handle(s1)
		c1.Write([]byte("*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n"))
		r := NewReader(c1)
		r.Read() // OK
		c1.Write([]byte("*2\r\n$3\r\nGET\r\n$2\r\nshared_key\r\n"))
		r.Read() // Null
		return c1, r
	}

	conns := make([]net.Conn, 3)
	readers := make([]*Reader, 3)
	for i := 0; i < 3; i++ {
		conns[i], readers[i] = createTrackingClient()
		defer conns[i].Close()
	}

	// Update key
	db.Set("shared_key", "updated", 0)

	// Verify all 3 clients receive invalidation
	for i := 0; i < 3; i++ {
		val, err := readers[i].Read()
		if err != nil || val.Type != Push || val.Array[1].Array[0].Bulk != "shared_key" {
			t.Fatalf("Client %d failed to receive invalidation for shared_key", i)
		}
	}
}

func TestSCC_Expiration(t *testing.T) {
	db := ledis.New(16)
	handler := NewHandler(db)

	c1, s1 := net.Pipe()
	go handler.Handle(s1)
	defer c1.Close()

	reader := NewReader(c1)
	c1.Write([]byte("*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n"))
	reader.Read()

	// Set with short TTL
	db.Set("expiry_key", "v1", 100*time.Millisecond)
	c1.Write([]byte("*2\r\n$3\r\nGET\r\n$10\r\nexpiry_key\r\n"))
	reader.Read()

	// Wait for expiration + buffer
	time.Sleep(200 * time.Millisecond)

	// Interaction to trigger lazy expiration if needed, or TTL check
	// Ledis.Get/TTL calls NotifyObservers on expiration
	db.Get("expiry_key")

	// Expect invalidation (async retry)
	doneExp := make(chan bool)
	go func() {
		for {
			val, err := reader.Read()
			if err != nil {
				return
			}
			if val.Type == Push && val.Array[1].Array[0].Bulk == "expiry_key" {
				doneExp <- true
				return
			}
		}
	}()

	select {
	case <-doneExp:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for invalidation on key expiration")
	}
}

func TestSCC_DelInvalidation(t *testing.T) {
	db := ledis.New(16)
	handler := NewHandler(db)

	c1, s1 := net.Pipe()
	go handler.Handle(s1)
	defer c1.Close()

	reader := NewReader(c1)
	c1.Write([]byte("*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n"))
	reader.Read()

	db.Set("del_key", "v1", 0)
	c1.Write([]byte("*2\r\n$3\r\nGET\r\n$7\r\ndel_key\r\n"))
	reader.Read()

	// Explicit Delete
	db.Del("del_key")

	// Expect invalidation (async)
	done := make(chan bool)
	go func() {
		for {
			val, err := reader.Read()
			if err != nil {
				// Pipe closed or error
				return
			}
			if val.Type == Push && val.Array[1].Array[0].Bulk == "del_key" {
				done <- true
				return
			}
		}
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for invalidation on key deletion")
	}
}
