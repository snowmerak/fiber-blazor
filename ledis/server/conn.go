package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snowmerak/fiber-blazor/ledis"
)

type Handler struct {
	db *ledis.DistributedMap
}

func NewHandler(db *ledis.DistributedMap) *Handler {
	return &Handler{db: db}
}

type Client struct {
	conn   net.Conn
	db     *ledis.DistributedMap
	writer *Writer
	mu     sync.Mutex // Protects writer and basic state

	id       int64
	tracking bool

	// Transaction State
	txMu     sync.Mutex
	watching map[string]bool
	inTx     bool
	dirty    bool
	txQueue  [][]string
}

func (c *Client) Invalidate(key string) {
	// 1. Handle WATCH (Synchronous to ensure safety before EXEC)
	c.txMu.Lock()
	if c.watching[key] {
		c.dirty = true
	}
	c.txMu.Unlock()

	// 2. Handle Client Tracking (SCC) - Asynchronous to avoid deadlock
	// We read c.tracking carefully; technically racy if modified concurrently,
	// but tracking assumes mostly atomic ON/OFF.
	// For strictness, could protect with mu, but Invalidate is called from DB locks.
	// We'll proceed optimistically or use atomic load if needed.
	// Given SCC is "Server-Assisted" and allows some laxity, current async approach is fine.

	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.tracking {
			return
		}

		// RESP3 Push: >2 \r\n $10 \r\n invalidate \r\n *1 \r\n $keylen \r\n key
		c.writer.WritePush(2)
		c.writer.WriteBulkString("invalidate")
		c.writer.WriteArray(1)
		c.writer.WriteBulkString(key)
	}()
}

func (h *Handler) Handle(conn net.Conn) {
	defer conn.Close()

	client := &Client{
		conn:     conn,
		db:       h.db,
		writer:   NewWriter(conn),
		id:       time.Now().UnixNano(),
		watching: make(map[string]bool),
	}

	h.db.RegisterObserver(client)
	defer h.db.UnregisterObserver(client)

	reader := NewReader(conn)

	for {
		val, err := reader.Read()
		if err != nil {
			return
		}

		if val.Type != Array {
			client.writeError("ERR request must be Array of Bulk Strings")
			continue
		}

		if len(val.Array) == 0 {
			continue
		}

		cmdName := ""
		switch val.Array[0].Type {
		case BulkString:
			cmdName = val.Array[0].Bulk
		case SimpleString:
			cmdName = val.Array[0].Str
		default:
			client.writeError("ERR Invalid command format")
			continue
		}

		args := make([]string, 0, len(val.Array)-1)
		for i := 1; i < len(val.Array); i++ {
			switch val.Array[i].Type {
			case BulkString:
				args = append(args, val.Array[i].Bulk)
			case SimpleString:
				args = append(args, val.Array[i].Str)
			case Integer:
				args = append(args, fmt.Sprintf("%d", val.Array[i].Num))
			default:
				args = append(args, "")
			}
		}

		client.execute(cmdName, args, client.writer, &client.mu)
	}
}

func (c *Client) writeError(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writer.WriteError(msg)
}

func (c *Client) execute(cmd string, args []string, w *Writer, mu *sync.Mutex) {

	// Use provided writer, or fall back to client writer (should always be provided now)
	wr := w
	if wr == nil {
		wr = c.writer
	}
	// Handle Transaction Commands first (they are never queued)
	upperCmd := strings.ToUpper(cmd)
	switch upperCmd {
	case "WATCH":
		c.txMu.Lock()
		defer c.txMu.Unlock()
		if c.inTx {
			c.mu.Lock()
			c.writer.WriteError("ERR WATCH inside MULTI is not allowed")
			c.mu.Unlock()
			return
		}
		for _, key := range args {
			c.watching[key] = true
		}
		c.mu.Lock()
		c.writer.WriteSimpleString("OK")
		c.mu.Unlock()
		return
	case "UNWATCH":
		c.txMu.Lock()
		defer c.txMu.Unlock()
		c.watching = make(map[string]bool)
		c.dirty = false // Reset dirty state? Standard Redis says UNWATCH flushes watched keys.
		// Does UNWATCH reset dirty? Yes contextually for the client.
		c.mu.Lock()
		c.writer.WriteSimpleString("OK")
		c.mu.Unlock()
		return
	case "MULTI":
		c.txMu.Lock()
		defer c.txMu.Unlock()
		if c.inTx {
			c.mu.Lock()
			c.writer.WriteError("ERR MULTI calls can not be nested")
			c.mu.Unlock()
			return
		}
		c.inTx = true
		c.txQueue = make([][]string, 0)
		c.mu.Lock()
		c.writer.WriteSimpleString("OK")
		c.mu.Unlock()
		return
	case "DISCARD":
		c.txMu.Lock()
		defer c.txMu.Unlock()
		if !c.inTx {
			c.mu.Lock()
			c.writer.WriteError("ERR DISCARD without MULTI")
			c.mu.Unlock()
			return
		}
		c.inTx = false
		c.txQueue = nil
		c.watching = make(map[string]bool)
		c.dirty = false
		c.mu.Lock()
		c.writer.WriteSimpleString("OK")
		c.mu.Unlock()
		return
	case "EXEC":
		c.txMu.Lock()
		if !c.inTx {
			c.txMu.Unlock()
			c.mu.Lock()
			c.writer.WriteError("ERR EXEC without MULTI")
			c.mu.Unlock()
			return
		}
		if c.dirty {
			c.txMu.Unlock()
			// Transaction failed
			c.txMu.Lock() // Re-acquire to clear state
			c.inTx = false
			c.txQueue = nil
			c.watching = make(map[string]bool)
			c.dirty = false
			c.txMu.Unlock()

			c.mu.Lock()
			c.writer.WriteNull() // Null array/bulk for failure
			c.mu.Unlock()
			return
		}

		// Execute Queue
		queue := c.txQueue
		c.inTx = false
		c.txQueue = nil
		c.watching = make(map[string]bool)
		c.dirty = false
		c.txMu.Unlock()

		// Analyze queue for concurrency
		// We can group commands by Shard Index.
		// If a command touches multiple shards (e.g. MSET), or no keys, or unknown keys,
		// we must treat it as a barrier and execute sequentially (or all parallel groups must finish first).
		// For simplicity in V1:
		// If ANY command in the queue is "unsafe" for parallelism, fallback to full sequential.
		// Unsafe: MSET, MGET, FLUSHDB, KEYS, etc. (Multi-key or global)
		// Safe: SET, GET, INCR, L* (single key), H* (single key), Z* (single key)

		canParallelize := true
		// Map from Queue Index -> Shard ID. -1 if unknown/global.
		cmdShards := make([]int, len(queue))

		for i, q := range queue {
			cmd := strings.ToUpper(q[0])
			args := q[1:]
			key := ""

			// Determine primary key
			switch cmd {
			case "SET", "GET", "INCR", "DECR",
				"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
				"HSET", "HGET", "HDEL", "HLEN", "HGETALL",
				"SADD", "SREM", "SMEMBERS", "SISMEMBER",
				"ZADD", "ZRANGE",
				"SETBIT", "GETBIT", "BITCOUNT",
				"XADD": // Stream
				if len(args) > 0 {
					key = args[0]
				}
			case "TTL", "EXISTS": // Read-only but single key
				if len(args) > 0 {
					key = args[0]
				}
			default:
				// MSET, MGET, DEL (multi-key), PUBLISH (channel is key? yes, but pubsub is global-ish in this impl? no, localized by channel key hash usually. But let's be safe), PING, ECHO
				// DEL is multi-key in args.
				canParallelize = false
			}

			if !canParallelize {
				break
			}

			if key != "" {
				cmdShards[i] = c.db.GetShardIndex(key)
			} else {
				// No key? e.g. random command or empty args. Safe to parallelize?
				// Better safe than sorry.
				canParallelize = false
			}
		}

		c.mu.Lock()
		c.writer.WriteArray(len(queue))
		c.mu.Unlock()

		if !canParallelize {
			// Sequential Fallback
			for _, q := range queue {
				c.execute(q[0], q[1:], c.writer, &c.mu)
			}
			return
		}

		// Parallel Execution
		// Group by Shard
		shardCmds := make(map[int][]int)
		for i, shardID := range cmdShards {
			shardCmds[shardID] = append(shardCmds[shardID], i)
		}

		// We'll create a buffer for each command in queue to capture output
		results := make([]*bytes.Buffer, len(queue))
		var wg sync.WaitGroup

		for shardID, indices := range shardCmds {
			// Launch goroutine for this shard
			wg.Add(1)
			go func(sid int, idxs []int) {
				defer wg.Done()
				for _, idx := range idxs {
					// Prepare buffer
					buf := new(bytes.Buffer)
					results[idx] = buf

					// Execute without client lock, writing to buffer
					// Note: we pass 'nil' for mutex because we don't want 'execute' to lock c.mu
					// 'c.execute' handles parsing and calling DB. DB calls handle their own locking.
					// c.writer is NOT used. We pass a new Writer wrapping our buffer.
					q := queue[idx]

					bufferedWriter := NewWriter(buf)

					// Lock is NOT held here.
					c.execute(q[0], q[1:], bufferedWriter, nil)
				}
			}(shardID, indices)
		}

		wg.Wait()

		// Write aggregated results to client sequentially
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, buf := range results {
			if _, err := io.Copy(c.writer.writer, buf); err != nil {
				// If writing fails, connection is probably dead
				return
			}
		}
		return
	}

	// Queue if inside Transaction
	c.txMu.Lock()
	if c.inTx {
		c.txQueue = append(c.txQueue, append([]string{cmd}, args...))
		c.txMu.Unlock()
		if mu != nil {
			mu.Lock()
		}
		wr.WriteSimpleString("QUEUED")
		if mu != nil {
			mu.Unlock()
		}
		return
	}
	c.txMu.Unlock()

	// Normal Execution
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	// wr is already set to w or c.writer
	// But wait, the original code had `c.mu.Lock()` here for "Normal Execution".
	// My signature change `execute(..., mu)` handles this locking logic at the START of the function.
	// So we don't need another Lock() here, IF we assume the entire function execution is atomic under `mu`.
	// However, my internal `mu` logic was:
	// if mu != nil { mu.Lock(); defer mu.Unlock() }
	// This covers the whole function.
	// So we don't need explicit locking here.
	// BUT, `wr` usage below needs to be safe. It is safe if covered by `mu`.

	switch upperCmd {
	case "HELLO":
		// Expecting "HELLO 3"
		ver := "3"
		if len(args) > 0 {
			ver = args[0]
		}
		if ver == "3" {
			wr.WriteMap(7)
			wr.WriteBulkString("server")
			wr.WriteBulkString("redis")
			wr.WriteBulkString("version")
			wr.WriteBulkString("7.2.4")
			wr.WriteBulkString("proto")
			wr.WriteInteger(3)
			wr.WriteBulkString("id")
			wr.WriteInteger(4) // Use a fixed small ID for handshake
			wr.WriteBulkString("mode")
			wr.WriteBulkString("standalone")
			wr.WriteBulkString("role")
			wr.WriteBulkString("master")
			wr.WriteBulkString("modules")
			wr.WriteArray(0)
		} else {
			wr.WriteArray(14)
			wr.WriteBulkString("server")
			wr.WriteBulkString("redis")
			wr.WriteBulkString("version")
			wr.WriteBulkString("7.2.4")
			wr.WriteBulkString("proto")
			wr.WriteInteger(2)
			wr.WriteBulkString("id")
			wr.WriteInteger(4)
			wr.WriteBulkString("mode")
			wr.WriteBulkString("standalone")
			wr.WriteBulkString("role")
			wr.WriteBulkString("master")
			wr.WriteBulkString("modules")
			wr.WriteArray(0)
		}
	case "CLIENT":
		if len(args) > 0 {
			sub := strings.ToUpper(args[0])
			if sub == "ID" {
				wr.WriteInteger(c.id)
				return
			}
			if sub == "INFO" {
				info := fmt.Sprintf("id=%d addr=%s name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 events=r cmd=client", c.id, c.conn.RemoteAddr())
				wr.WriteBulkString(info)
				return
			}
			if sub == "SETNAME" {
				if len(args) < 2 {
					wr.WriteError("ERR syntax error")
					return
				}
				wr.WriteSimpleString("OK")
				return
			}
			if sub == "GETNAME" {
				wr.WriteNull()
				return
			}
			if sub == "TRACKING" {
				// CLIENT TRACKING ON/OFF ...
				if len(args) < 2 {
					wr.WriteError("ERR syntax error")
					return
				}
				toggle := strings.ToUpper(args[1])
				switch toggle {
				case "ON":
					c.tracking = true
					wr.WriteSimpleString("OK")
				case "OFF":
					c.tracking = false
					wr.WriteSimpleString("OK")
				default:
					wr.WriteError("ERR syntax error")
				}
				return
			}
		}
		wr.WriteError("ERR subcommand not supported")

	// --- Generic ---
	case "PING":
		if len(args) > 0 {
			wr.WriteBulkString(args[0])
		} else {
			wr.WriteSimpleString("PONG")
		}
	case "DEL":
		if len(args) < 1 {
			wr.WriteError("ERR wrong number of arguments for 'del' command")
			return
		}
		count := 0
		for _, key := range args {
			if c.db.Exists(key) {
				c.db.Del(key)
				count++
			}
		}
		wr.WriteInteger(int64(count))
	case "EXISTS":
		if len(args) < 1 {
			wr.WriteError("ERR wrong number of arguments for 'exists' command")
			return
		}
		count := 0
		for _, key := range args {
			if c.tracking {
				c.db.Track(key, c)
			}
			if c.db.Exists(key) {
				count++
			}
		}
		wr.WriteInteger(int64(count))
	case "TTL":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'ttl' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		ttl := c.db.TTL(args[0])
		wr.WriteInteger(int64(ttl.Seconds()))

	// --- String ---
	case "SET":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		c.db.Set(args[0], args[1], 0)
		wr.WriteSimpleString("OK")
	case "GET":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'get' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		val, exists := c.db.Get(args[0])
		if !exists {
			wr.WriteNull()
			return
		}
		if s, ok := val.(string); ok {
			wr.WriteBulkString(s)
		} else {
			wr.WriteBulkString(fmt.Sprintf("%v", val))
		}
	case "MSET":
		if len(args) == 0 || len(args)%2 != 0 {
			wr.WriteError("ERR wrong number of arguments for 'mset' command")
			return
		}
		pairs := make(map[string]interface{})
		for i := 0; i < len(args); i += 2 {
			pairs[args[i]] = args[i+1]
		}
		c.db.MSet(pairs)
		wr.WriteSimpleString("OK")
	case "MGET":
		if len(args) == 0 {
			wr.WriteError("ERR wrong number of arguments for 'mget' command")
			return
		}
		vals := c.db.MGet(args...)
		wr.WriteArray(len(vals))
		for i, v := range vals {
			if c.tracking {
				c.db.Track(args[i], c)
			}
			if v == nil {
				wr.WriteNull()
			} else {
				if s, ok := v.(string); ok {
					wr.WriteBulkString(s)
				} else {
					wr.WriteBulkString(fmt.Sprintf("%v", v))
				}
			}
		}
	case "INCR":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'incr' command")
			return
		}
		val, err := c.db.Incr(args[0])
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(val)
		}
	case "DECR":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'decr' command")
			return
		}
		val, err := c.db.Decr(args[0])
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(val)
		}

	// --- List ---
	case "LPUSH":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'lpush' command")
			return
		}
		count, err := c.db.LPush(args[0], stringToInterfaceSlice(args[1:])...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(int64(count))
		}
	case "RPUSH":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'rpush' command")
			return
		}
		count, err := c.db.RPush(args[0], stringToInterfaceSlice(args[1:])...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(int64(count))
		}
	case "LPOP":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'lpop' command")
			return
		}
		// LPOP is a write command, but it returns the value, so does it count as a read?
		// In Redis, LPOP triggers invalidation for others, but does it register tracking?
		// Actually, if tracking is ON, any command that returns data keys *could* be tracked,
		// but typically only "ReadOnly" commands are tracked.
		// However, in Redis 6+, even write commands returning data might not be tracked.
		// Wait, LPOP invalidates the key.
		// If I do LPOP k1, I get v1. Does it mean I'm watching k1?
		// No, LPOP modifies k1.
		// Only commands that don't modify the key generally register tracking.
		// So we skip LPOP/RPOP.

		val, err := c.db.LPop(args[0])
		if err != nil {
			wr.WriteNull()
		} else {
			if s, ok := val.(string); ok {
				wr.WriteBulkString(s)
			} else {
				wr.WriteBulkString(fmt.Sprintf("%v", val))
			}
		}
	case "RPOP":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'rpop' command")
			return
		}
		val, err := c.db.RPop(args[0])
		if err != nil {
			wr.WriteNull()
		} else {
			if s, ok := val.(string); ok {
				wr.WriteBulkString(s)
			} else {
				wr.WriteBulkString(fmt.Sprintf("%v", val))
			}
		}
	case "LLEN":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'llen' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		l, err := c.db.LLen(args[0])
		if err != nil {
			wr.WriteInteger(0)
		} else {
			wr.WriteInteger(int64(l))
		}
	case "LRANGE":
		if len(args) != 3 {
			wr.WriteError("ERR wrong number of arguments for 'lrange' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		start, err1 := strconv.ParseInt(args[1], 10, 64)
		stop, err2 := strconv.ParseInt(args[2], 10, 64)
		if err1 != nil || err2 != nil {
			wr.WriteError("ERR value is not an integer or out of range")
			return
		}
		vals, err := c.db.LRange(args[0], int(start), int(stop))
		if err != nil {
			wr.WriteArray(0)
		} else {
			wr.WriteArray(len(vals))
			for _, v := range vals {
				if s, ok := v.(string); ok {
					wr.WriteBulkString(s)
				} else {
					wr.WriteBulkString(fmt.Sprintf("%v", v))
				}
			}
		}

	// --- Hash ---
	case "HSET":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			wr.WriteError("ERR wrong number of arguments for 'hset' command")
			return
		}
		count := 0
		for i := 1; i < len(args); i += 2 {
			n, err := c.db.HSet(args[0], args[i], args[i+1])
			if err == nil {
				count += int(n)
			}
		}
		wr.WriteInteger(int64(count))
	case "HGET":
		if len(args) != 2 {
			wr.WriteError("ERR wrong number of arguments for 'hget' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		val, err := c.db.HGet(args[0], args[1])
		if err != nil {
			wr.WriteNull()
		} else {
			if s, ok := val.(string); ok {
				wr.WriteBulkString(s)
			} else {
				wr.WriteBulkString(fmt.Sprintf("%v", val))
			}
		}
	case "HDEL":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'hdel' command")
			return
		}
		count := 0
		for i := 1; i < len(args); i++ {
			n, err := c.db.HDel(args[0], args[i])
			if err == nil {
				count += int(n)
			}
		}
		wr.WriteInteger(int64(count))
	case "HLEN":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'hlen' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		l, err := c.db.HLen(args[0])
		if err != nil {
			wr.WriteInteger(0)
		} else {
			wr.WriteInteger(int64(l))
		}
	case "HGETALL":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'hgetall' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		kv, err := c.db.HGetAll(args[0])
		if err != nil {
			wr.WriteArray(0)
		} else {
			wr.WriteArray(len(kv) * 2)
			for k, v := range kv {
				wr.WriteBulkString(k)
				if s, ok := v.(string); ok {
					wr.WriteBulkString(s)
				} else {
					wr.WriteBulkString(fmt.Sprintf("%v", v))
				}
			}
		}

	// --- Set ---
	case "SADD":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'sadd' command")
			return
		}
		count, err := c.db.SAdd(args[0], stringToInterfaceSlice(args[1:])...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(int64(count))
		}
	case "SREM":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'srem' command")
			return
		}
		count, err := c.db.SRem(args[0], stringToInterfaceSlice(args[1:])...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(int64(count))
		}
	case "SMEMBERS":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'smembers' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		members, err := c.db.SMembers(args[0])
		if err != nil {
			wr.WriteArray(0)
		} else {
			wr.WriteArray(len(members))
			for _, m := range members {
				if s, ok := m.(string); ok {
					wr.WriteBulkString(s)
				} else {
					wr.WriteBulkString(fmt.Sprintf("%v", m))
				}
			}
		}
	case "SISMEMBER":
		if len(args) != 2 {
			wr.WriteError("ERR wrong number of arguments for 'sismember' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		isMember, err := c.db.SIsMember(args[0], args[1])
		if err != nil {
			wr.WriteInteger(0)
		} else {
			if isMember {
				wr.WriteInteger(1)
			} else {
				wr.WriteInteger(0)
			}
		}
	// --- Sorted Set ---
	case "ZADD":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			wr.WriteError("ERR wrong number of arguments for 'zadd' command")
			return
		}
		added := 0
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				wr.WriteError("ERR value is not a valid float")
				return
			}
			member := args[i+1]
			count, err := c.db.ZAdd(args[0], score, member)
			if err == nil {
				added += count
			}
		}
		wr.WriteInteger(int64(added))
	case "ZRANGE":
		if len(args) != 3 {
			wr.WriteError("ERR wrong number of arguments for 'zrange' command")
			return
		}
		if c.tracking {
			c.db.Track(args[0], c)
		}
		start, err1 := strconv.ParseInt(args[1], 10, 64)
		stop, err2 := strconv.ParseInt(args[2], 10, 64)
		if err1 != nil || err2 != nil {
			wr.WriteError("ERR value is not an integer")
			return
		}
		res, err := c.db.ZRange(args[0], start, stop, false)
		if err != nil {
			wr.WriteArray(0)
		} else {
			wr.WriteArray(len(res))
			for _, item := range res {
				if s, ok := item.(string); ok {
					wr.WriteBulkString(s)
				} else if f, ok := item.(float64); ok {
					wr.WriteBulkString(fmt.Sprintf("%g", f))
				} else {
					wr.WriteBulkString(fmt.Sprintf("%v", item))
				}
			}
		}

	// --- Bitmap ---
	case "SETBIT":
		if len(args) != 3 {
			wr.WriteError("ERR wrong number of arguments for 'setbit' command")
			return
		}
		offset, err1 := strconv.ParseUint(args[1], 10, 64)
		val, err2 := strconv.ParseInt(args[2], 10, 64)
		if err1 != nil || err2 != nil {
			wr.WriteError("ERR bit or offset is not an integer")
			return
		}
		oldVal, err := c.db.SetBit(args[0], offset, int(val))
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteInteger(int64(oldVal))
		}
	case "GETBIT":
		if len(args) != 2 {
			wr.WriteError("ERR wrong number of arguments for 'getbit' command")
			return
		}
		offset, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			wr.WriteError("ERR offset is not an integer")
			return
		}
		val, err := c.db.GetBit(args[0], offset)
		if err != nil {
			wr.WriteInteger(0)
		} else {
			wr.WriteInteger(int64(val))
		}
	case "BITCOUNT":
		if len(args) != 1 {
			if len(args) > 1 {
				wr.WriteError("ERR syntax error (arguments not supported for bitcount yet)")
				return
			}
			wr.WriteError("ERR wrong number of arguments for 'bitcount' command")
			return
		}
		count, err := c.db.BitCount(args[0])
		if err != nil {
			wr.WriteInteger(0)
		} else {
			wr.WriteInteger(int64(count))
		}

	// --- Pub/Sub ---
	case "PUBLISH":
		if len(args) != 2 {
			wr.WriteError("ERR wrong number of arguments for 'publish' command")
			return
		}
		count := c.db.Publish(args[0], args[1])
		wr.WriteInteger(count)

	// --- Stream ---
	case "XADD":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'xadd' command")
			return
		}
		id, err := c.db.XAdd(args[0], args[1], args[2:]...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			wr.WriteBulkString(id)
		}

	default:
		wr.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

func stringToInterfaceSlice(args []string) []interface{} {
	iface := make([]interface{}, len(args))
	for i, v := range args {
		iface[i] = v
	}
	return iface
}
