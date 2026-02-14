package server

import (
	"fmt"
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
	mu     sync.Mutex

	id       int64
	tracking bool
}

func (c *Client) Invalidate(key string) {
	if !c.tracking {
		return
	}

	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Re-check tracking inside lock
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
		conn:   conn,
		db:     h.db,
		writer: NewWriter(conn),
		id:     time.Now().UnixNano(),
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

		client.execute(cmdName, args)
	}
}

func (c *Client) writeError(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writer.WriteError(msg)
}

func (c *Client) execute(cmd string, args []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wr := c.writer

	switch strings.ToUpper(cmd) {
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
				if toggle == "ON" {
					c.tracking = true
					wr.WriteSimpleString("OK")
				} else if toggle == "OFF" {
					c.tracking = false
					wr.WriteSimpleString("OK")
				} else {
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
		for _, v := range vals {
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
