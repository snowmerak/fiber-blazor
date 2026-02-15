package server

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/snowmerak/fiber-blazor/ledis"
)

func TestGoRedisIntegration(t *testing.T) {
	db := ledis.New(16)
	rdb := NewGoRedisClient(db)
	defer rdb.Close()

	ctx := context.Background()

	// PING
	val, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if val != "PONG" {
		t.Errorf("PING expected PONG, got %s", val)
	}

	// SET
	err = rdb.Set(ctx, "mykey", "myval", 0).Err()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	// GET
	val, err = rdb.Get(ctx, "mykey").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "myval" {
		t.Errorf("GET expected myval, got %s", val)
	}
}

func TestRueidisIntegration(t *testing.T) {
	db := ledis.New(16)
	client, err := NewRueidisClient(db)
	if err != nil {
		t.Fatalf("NewRueidisClient failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// PING
	err = client.Do(ctx, client.B().Ping().Build()).Error()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}

	// SET
	err = client.Do(ctx, client.B().Set().Key("rueidis_key").Value("rueidis_val").Build()).Error()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	// GET
	val, err := client.Do(ctx, client.B().Get().Key("rueidis_key").Build()).ToString()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "rueidis_val" {
		t.Errorf("GET expected rueidis_val, got %s", val)
	}
}

func TestStreamTrimIntegration(t *testing.T) {
	db := ledis.New(16)
	rdb := NewGoRedisClient(db)
	defer rdb.Close()
	defer db.Close()

	ctx := context.Background()

	// XADD with MAXLEN
	// go-redis XAddArgs: Stream, MaxLen, MaxLenApprox, ID, Values
	for i := 0; i < 5; i++ {
		err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream_trim",
			MaxLen: 3,
			ID:     "*",
			Values: map[string]interface{}{"key": "val"},
		}).Err()
		if err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}

	// Check Len
	l, err := rdb.XLen(ctx, "stream_trim").Result()
	if err != nil {
		t.Fatalf("XLen failed: %v", err)
	}
	if l != 3 {
		t.Errorf("Expected len 3 (capped by MAXLEN), got %d", l)
	}

	// XTRIM
	// Explicitly trim to 1
	n, err := rdb.XTrimMaxLen(ctx, "stream_trim", 1).Result()
	if err != nil {
		t.Fatalf("XTrim failed: %v", err)
	}
	if n != 2 { // 3 - 1 = 2 deleted
		t.Errorf("Expected 2 deleted, got %d", n)
	}

	l, err = rdb.XLen(ctx, "stream_trim").Result()
	if err != nil {
		t.Fatalf("XLen after trim failed: %v", err)
	}
	if l != 1 {
		t.Errorf("Expected len 1 after XTRIM, got %d", l)
	}
}
