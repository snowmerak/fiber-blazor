package integrations

import (
	"context"
	"testing"

	"github.com/snowmerak/fiber-blazor/ledis"
	"github.com/snowmerak/fiber-blazor/ledis/server"
)

func TestGoRedisIntegration(t *testing.T) {
	db := ledis.New(16)
	rdb := server.NewGoRedisClient(db)
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
	client, err := server.NewRueidisClient(db)
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
