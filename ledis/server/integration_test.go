package server

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/snowmerak/fiber-blazor/ledis"
)

func TestComprehensiveIntegration(t *testing.T) {
	db := ledis.New(16)
	rdb := NewGoRedisClient(db)
	defer rdb.Close()
	ctx := context.Background()

	t.Run("Strings", func(t *testing.T) {
		// SET/GET
		if err := rdb.Set(ctx, "skey", "sval", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if val, err := rdb.Get(ctx, "skey").Result(); err != nil || val != "sval" {
			t.Fatalf("GET failed: %v, %s", err, val)
		}
		// INCR / DECR
		rdb.Set(ctx, "ctr", "10", 0)
		if val, err := rdb.Incr(ctx, "ctr").Result(); err != nil || val != 11 {
			t.Fatalf("INCR failed: %v, %d", err, val)
		}
		if val, err := rdb.Decr(ctx, "ctr").Result(); err != nil || val != 10 {
			t.Fatalf("DECR failed: %v, %d", err, val)
		}
		// MSET / MGET
		if err := rdb.MSet(ctx, "k1", "v1", "k2", "v2").Err(); err != nil {
			t.Fatal(err)
		}
		vals, err := rdb.MGet(ctx, "k1", "k2", "missing").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 3 {
			t.Fatalf("MGET len mismatch")
		}
		if vals[0] != "v1" || vals[1] != "v2" || vals[2] != nil {
			t.Fatalf("MGET values mismatch: %v", vals)
		}
	})

	t.Run("Lists", func(t *testing.T) {
		rdb.Del(ctx, "mylist")
		// LPUSH
		if err := rdb.LPush(ctx, "mylist", "world", "hello").Err(); err != nil {
			t.Fatal(err)
		}
		// LRANGE
		vals, err := rdb.LRange(ctx, "mylist", 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		expected := []string{"hello", "world"}
		if !reflect.DeepEqual(vals, expected) {
			t.Fatalf("LRANGE mismatch: got %v, want %v", vals, expected)
		}
		// LLEN
		if l, err := rdb.LLen(ctx, "mylist").Result(); err != nil || l != 2 {
			t.Fatalf("LLEN failed: %v, %d", err, l)
		}
		// RPOP
		if val, err := rdb.RPop(ctx, "mylist").Result(); err != nil || val != "world" {
			t.Fatalf("RPOP failed: %v, %s", err, val)
		}
	})

	t.Run("Hashes", func(t *testing.T) {
		rdb.Del(ctx, "myhash")
		// HSET
		if err := rdb.HSet(ctx, "myhash", "f1", "v1", "f2", "v2").Err(); err != nil {
			t.Fatal(err)
		}
		// HGET
		if val, err := rdb.HGet(ctx, "myhash", "f1").Result(); err != nil || val != "v1" {
			t.Fatalf("HGET failed: %v, %s", err, val)
		}
		// HGETALL
		all, err := rdb.HGetAll(ctx, "myhash").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 || all["f1"] != "v1" || all["f2"] != "v2" {
			t.Fatalf("HGETALL mismatch: %v", all)
		}
		// HLEN
		if l, err := rdb.HLen(ctx, "myhash").Result(); err != nil || l != 2 {
			t.Fatalf("HLEN failed: %v, %d", err, l)
		}
	})

	t.Run("Sets", func(t *testing.T) {
		rdb.Del(ctx, "myset")
		// SADD
		if err := rdb.SAdd(ctx, "myset", "a", "b", "c").Err(); err != nil {
			t.Fatal(err)
		}
		// SISMEMBER
		if is, err := rdb.SIsMember(ctx, "myset", "a").Result(); err != nil || !is {
			t.Fatalf("SISMEMBER a failed")
		}
		if is, err := rdb.SIsMember(ctx, "myset", "z").Result(); err != nil || is {
			t.Fatalf("SISMEMBER z failed")
		}
		// SMEMBERS
		members, err := rdb.SMembers(ctx, "myset").Result()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(members)
		expected := []string{"a", "b", "c"}
		if !reflect.DeepEqual(members, expected) {
			t.Fatalf("SMEMBERS mismatch: got %v, want %v", members, expected)
		}
	})

	t.Run("SortedSets", func(t *testing.T) {
		rdb.Del(ctx, "myzset")
		// ZADD
		members := []redis.Z{
			{Score: 1.0, Member: "one"},
			{Score: 2.0, Member: "two"},
			{Score: 3.0, Member: "three"},
		}
		if err := rdb.ZAdd(ctx, "myzset", members...).Err(); err != nil {
			t.Fatal(err)
		}
		// ZRANGE
		vals, err := rdb.ZRange(ctx, "myzset", 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		expected := []string{"one", "two", "three"}
		if !reflect.DeepEqual(vals, expected) {
			t.Fatalf("ZRANGE mismatch: got %v, want %v", vals, expected)
		}
	})

	t.Run("Bitmaps", func(t *testing.T) {
		rdb.Del(ctx, "mybit")
		// SETBIT
		if err := rdb.SetBit(ctx, "mybit", 10, 1).Err(); err != nil {
			t.Fatal(err)
		}
		// GETBIT
		if val, err := rdb.GetBit(ctx, "mybit", 10).Result(); err != nil || val != 1 {
			t.Fatalf("GETBIT 10 failed")
		}
		if val, err := rdb.GetBit(ctx, "mybit", 0).Result(); err != nil || val != 0 {
			t.Fatalf("GETBIT 0 failed")
		}
		// BITCOUNT
		if count, err := rdb.BitCount(ctx, "mybit", nil).Result(); err != nil || count != 1 {
			t.Fatalf("BITCOUNT failed: %v, %d", err, count)
		}
	})

	t.Run("PubSub", func(t *testing.T) {
		// Just testing PUBLISH for now as SUBSCRIBE blocks
		// PUBLISH
		if count, err := rdb.Publish(ctx, "chan1", "msg").Result(); err != nil {
			t.Fatal(err)
		} else if count != 0 {
			// No subscribers yet
			t.Fatalf("Expected 0 subscribers, got %d", count)
		}
		// Real pubsub test would need a separate goroutine consuming msgChan
	})

	t.Run("Streams", func(t *testing.T) {
		rdb.Del(ctx, "mystream")
		// XADD
		// go-redis XAddArgs
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "mystream",
			Values: map[string]any{"f1": "v1"},
		}).Result()
		if err != nil {
			t.Fatal(err)
		}
		if id == "" {
			t.Fatal("XADD returned empty ID")
		}
		fmt.Println("XADD ID:", id)
	})

	// Complex case: concurrent operations via map
	t.Run("Concurrent", func(t *testing.T) {
		done := make(chan bool)
		for i := range 10 {
			go func(id int) {
				key := fmt.Sprintf("ckey:%d", id)
				rdb.Set(ctx, key, id, 0)
				rdb.Get(ctx, key)
				done <- true
			}(i)
		}
		for range 10 {
			<-done
		}
	})
}
