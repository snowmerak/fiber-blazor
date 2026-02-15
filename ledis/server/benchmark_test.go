package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/snowmerak/fiber-blazor/ledis"
)

// Benchmark setup:
// 1. Start Ledis Server on random port.
// 2. Connect to Ledis.
// 3. Connect to Redis (localhost:6379) - Fail gracefully if not available.

func setupLedisBenchmark(b *testing.B) (*redis.Client, func()) {
	// Start Ledis
	db := ledis.New(1024) // Larger size for bench? 1024 slots is fine.
	handler := NewHandler(db)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler.Handle(conn)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
	})

	return client, func() {
		client.Close()
		ln.Close()
	}
}

func setupRedisBenchmark(b *testing.B) (*redis.Client, func()) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available at %s: %v", redisAddr, err)
		return nil, nil
	}

	// FLUSHDB to start clean? Maybe risky if user has data.
	// Uses a random prefix or DB 1?
	// Let's use DB 1
	client = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   1,
	})
	client.FlushDB(context.Background())

	return client, func() {
		client.Close()
	}
}

// Benchmarks

func runBenchmark(b *testing.B, name string, op func(b *testing.B, client *redis.Client)) {
	b.Run("Ledis/"+name, func(b *testing.B) {
		client, cleanup := setupLedisBenchmark(b)
		defer cleanup()
		op(b, client)
	})

	b.Run("Redis/"+name, func(b *testing.B) {
		client, cleanup := setupRedisBenchmark(b)
		if client == nil {
			return
		}
		defer cleanup()
		op(b, client)
	})
}

func BenchmarkCommands(b *testing.B) {
	ctx := context.Background()

	// 1. SET
	runBenchmark(b, "SET", func(b *testing.B, client *redis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Set(ctx, "key", "value", 0).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 2. GET
	runBenchmark(b, "GET", func(b *testing.B, client *redis.Client) {
		client.Set(ctx, "bench_get", "value", 0)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := client.Get(ctx, "bench_get").Result(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 3. INCR
	runBenchmark(b, "INCR", func(b *testing.B, client *redis.Client) {
		client.Set(ctx, "bench_incr", "0", 0)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := client.Incr(ctx, "bench_incr").Result(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 4. LPUSH
	runBenchmark(b, "LPUSH", func(b *testing.B, client *redis.Client) {
		client.Del(ctx, "bench_list")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.LPush(ctx, "bench_list", "val").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 5. LPOP
	runBenchmark(b, "LPOP", func(b *testing.B, client *redis.Client) {
		// Pre-fill list somewhat to avoid running out?
		// If N is large, we need to push first.
		// Testing mostly write throughput if we alternate?
		// Let's just push N items first.
		// Note: Pushing N items takes time, exclusion from timer is tricky.
		// We'll standard PUSH/POP cycle in loop
		b.StopTimer()
		pipe := client.Pipeline()
		for range 1000 {
			pipe.LPush(ctx, "bench_lpop", "v")
		}
		// Batch push if needed, but for benchmark we want pure LPOP speed.
		// If list empty, LPOP returns nil/error fast.
		// Strategy: LPUSH then LPOP in loop.
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			client.LPush(ctx, "bench_lpop_cycle", "v")
			client.LPop(ctx, "bench_lpop_cycle")
		}
	})

	// 6. HSET
	runBenchmark(b, "HSET", func(b *testing.B, client *redis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.HSet(ctx, "bench_hash", "f1", "v1").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 7. HGET
	runBenchmark(b, "HGET", func(b *testing.B, client *redis.Client) {
		client.HSet(ctx, "bench_hash_get", "f1", "v1")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := client.HGet(ctx, "bench_hash_get", "f1").Result(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 8. SADD
	runBenchmark(b, "SADD", func(b *testing.B, client *redis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Use constant member to stress adding existing
			if err := client.SAdd(ctx, "bench_set", "m1").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 9. SISMEMBER
	runBenchmark(b, "SISMEMBER", func(b *testing.B, client *redis.Client) {
		client.SAdd(ctx, "bench_set_is", "m1")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := client.SIsMember(ctx, "bench_set_is", "m1").Result(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 10. ZADD
	runBenchmark(b, "ZADD", func(b *testing.B, client *redis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.ZAdd(ctx, "bench_zset", redis.Z{Score: float64(i), Member: fmt.Sprintf("m%d", i)}).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 11. ZRANGE
	runBenchmark(b, "ZRANGE", func(b *testing.B, client *redis.Client) {
		b.StopTimer()
		// Fill some data
		for i := range 100 {
			client.ZAdd(ctx, "bench_zset_range", redis.Z{Score: float64(i), Member: fmt.Sprintf("m%d", i)})
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			if _, err := client.ZRange(ctx, "bench_zset_range", 0, 10).Result(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 12. PING
	runBenchmark(b, "PING", func(b *testing.B, client *redis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Ping(ctx).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestBenchmarkSetup(t *testing.T) {
	t.Log("Benchmark file is compiled")
}
