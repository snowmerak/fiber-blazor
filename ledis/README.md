# Ledis

Ledis is a high-performance, embedded Redis-compatible key-value store library for Go. It is designed to be used directly within Go applications or as a standalone server supporting the RESP (Redis Serialization Protocol).

## Features

- **High Performance**: Built with `sync.Map` sharding (1024 shards by default) and optimized with `ants` goroutine pool and `sync.Pool` for object reuse.
- **Redis Compatibility**: Supports a wide range of Redis data types and commands:
  - **String**: Get, Set, Incr, Decr, Append, MGet, MSet...
  - **List**: LPush, RPush, LPop, RPop, LRange, BLPop...
  - **Hash**: HGet, HSet, HGetAll, HKeys, HVals...
  - **Set**: SAdd, SRem, SMembers, SInter, SUnion, SDiff...
  - **ZSet (Sorted Set)**: ZAdd, ZRange, ZRem, ZRank, ZScore...
  - **Bitmap**: SetBit, GetBit, BitCount, BitOp...
  - **Stream**: XAdd, XRead, XRange...
  - **Pub/Sub**: Publish, Subscribe...
- **Active TTL Eviction**: Hybrid strategy using high-frequency probabilistic sampling and background sequential scanning to ensure memory efficiency.
- **Transaction Support**: Supports `MULTI`, `EXEC`, `DISCARD` for atomic operations.
- **Persistence**: (Currently In-Memory only, focused on speed).

## Installation

```bash
go get github.com/snowmerak/fiber-blazor/ledis
```

## Usage

### Embedded Library

```go
package main

import (
	"fmt"
	"time"
	"github.com/snowmerak/fiber-blazor/ledis"
)

func main() {
	// Initialize Ledis with default settings (1024 shards)
	db := ledis.New(0)
	defer db.Close()

	// Set a key
	db.Set("mykey", "hello", 0)

	// Get a key
	item, err := db.Get("mykey")
	if err == nil {
		fmt.Println(item.Str) // "hello"
	}

	// Use other data types
	db.RPush("mylist", "a", "b", "c")
	list, _ := db.LRange("mylist", 0, -1)
	fmt.Println(list) // [a b c]
}
```

### RESP Server

You can run Ledis as a Redis-compatible server (requires implementing server main loop using `server` package):

```go
package main

import (
	"github.com/snowmerak/fiber-blazor/ledis"
	"github.com/snowmerak/fiber-blazor/ledis/server"
)

func main() {
	db := ledis.New(0)
	// Example server initialization (see server/ package)
}
```

## Performance

Ledis is optimized for local loopback performance, outperforming standard Redis in serialization and execution speed for embedded use cases.
- **ZRANGE**: ~16µs (Ledis) vs ~230µs (Redis via go-redis)
- **SET**: ~14µs (Ledis) vs ~220µs (Redis via go-redis)

*Benchmarks run on local machine comparing Ledis server vs Redis server.*
