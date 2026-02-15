package server

import (
	"context"
	"fmt"
	"testing"

	// Added time import just in case, though maybe not used directly in this file if setups handle it?
	// Wait, BenchmarkRueidisCommands uses fmt.Sprintf.
	// LPOP bench uses loop.
	// setupRueidisRedisBenchmark uses time but that is in benchmark_test.go
	// runRueidisBenchmark uses testing.

	"github.com/redis/rueidis"
)

func runRueidisBenchmark(b *testing.B, name string, op func(b *testing.B, client rueidis.Client)) {
	b.Run("Ledis/"+name, func(b *testing.B) {
		client, cleanup := setupRueidisLedisBenchmark(b)
		defer cleanup()
		op(b, client)
	})

	b.Run("Redis/"+name, func(b *testing.B) {
		client, cleanup := setupRueidisRedisBenchmark(b)
		if client == nil {
			return
		}
		defer cleanup()
		op(b, client)
	})
}

func BenchmarkRueidisCommands(b *testing.B) {
	ctx := context.Background()

	// 1. SET
	runRueidisBenchmark(b, "SET", func(b *testing.B, client rueidis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Set().Key("key").Value("value").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 2. GET
	runRueidisBenchmark(b, "GET", func(b *testing.B, client rueidis.Client) {
		client.Do(ctx, client.B().Set().Key("bench_get").Value("value").Build())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Get().Key("bench_get").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 3. INCR
	runRueidisBenchmark(b, "INCR", func(b *testing.B, client rueidis.Client) {
		client.Do(ctx, client.B().Set().Key("bench_incr").Value("0").Build())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Incr().Key("bench_incr").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 4. LPUSH
	runRueidisBenchmark(b, "LPUSH", func(b *testing.B, client rueidis.Client) {
		client.Do(ctx, client.B().Del().Key("bench_list").Build())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Lpush().Key("bench_list").Element("val").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 5. LPOP
	runRueidisBenchmark(b, "LPOP", func(b *testing.B, client rueidis.Client) {
		// Fill some
		for range 1000 {
			client.Do(ctx, client.B().Lpush().Key("bench_lpop").Element("v").Build())
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Cyclic to avoid empty list too soon
			client.Do(ctx, client.B().Lpush().Key("bench_lpop_cycle").Element("v").Build())
			client.Do(ctx, client.B().Lpop().Key("bench_lpop_cycle").Build())
		}
	})

	// 6. HSET
	runRueidisBenchmark(b, "HSET", func(b *testing.B, client rueidis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Hset().Key("bench_hash").FieldValue().FieldValue("f1", "v1").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 7. HGET
	runRueidisBenchmark(b, "HGET", func(b *testing.B, client rueidis.Client) {
		client.Do(ctx, client.B().Hset().Key("bench_hash_get").FieldValue().FieldValue("f1", "v1").Build())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Hget().Key("bench_hash_get").Field("f1").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 8. SADD
	runRueidisBenchmark(b, "SADD", func(b *testing.B, client rueidis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Sadd().Key("bench_set").Member("m1").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 9. SISMEMBER
	runRueidisBenchmark(b, "SISMEMBER", func(b *testing.B, client rueidis.Client) {
		client.Do(ctx, client.B().Sadd().Key("bench_set_is").Member("m1").Build())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Sismember().Key("bench_set_is").Member("m1").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 10. ZADD
	runRueidisBenchmark(b, "ZADD", func(b *testing.B, client rueidis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Zadd().Key("bench_zset").ScoreMember().ScoreMember(float64(i), fmt.Sprintf("m%d", i)).Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 11. ZRANGE
	runRueidisBenchmark(b, "ZRANGE", func(b *testing.B, client rueidis.Client) {
		b.StopTimer()
		for i := range 100 {
			client.Do(ctx, client.B().Zadd().Key("bench_zset_range").ScoreMember().ScoreMember(float64(i), fmt.Sprintf("m%d", i)).Build())
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Zrange().Key("bench_zset_range").Min("0").Max("10").Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 12. PING
	runRueidisBenchmark(b, "PING", func(b *testing.B, client rueidis.Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
