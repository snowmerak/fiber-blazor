package server

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/snowmerak/fiber-blazor/ledis"
)

type Handler struct {
	db *ledis.DistributedMap
}

func NewHandler(db *ledis.DistributedMap) *Handler {
	return &Handler{db: db}
}

func (h *Handler) Handle(conn net.Conn) {
	defer conn.Close()

	reader := NewReader(conn)
	// Writer needed? Standard io.Writer is enough for helpers.
	// But we can wrap it.
	// writer := NewWriter(conn)
	// Wait, my Writer struct in resp.go has helpers that take receiver.
	// So I should use it.

	// Issue: resp.go Writer struct wasn't exported or completely defined in previous step?
	// I defined `type Writer struct`.
	// Let's assume it's there.

	for {
		val, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				// Log error?
				fmt.Println("Read error:", err)
			}
			return
		}

		// Expect Array of Bulk Strings
		if val.Type != Array {
			// Write error
			h.writeError(conn, "ERR request must be Array of Bulk Strings")
			continue
		}

		if len(val.Array) == 0 {
			continue // Empty command?
		}

		// Parse Command Name
		cmdName := ""
		switch val.Array[0].Type {
		case BulkString:
			cmdName = val.Array[0].Bulk
		case SimpleString:
			cmdName = val.Array[0].Str
		default:
			h.writeError(conn, "ERR Invalid command format")
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
				args = append(args, "") // Handle others?
			}
		}

		h.execute(conn, cmdName, args)
	}
}

func (h *Handler) writeError(w io.Writer, msg string) {
	w.Write([]byte(fmt.Sprintf("-%s\r\n", msg)))
}

func (h *Handler) execute(w io.Writer, cmd string, args []string) {
	wr := NewWriter(w)

	switch strings.ToUpper(cmd) {
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
			if h.db.Exists(key) {
				h.db.Del(key)
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
			if h.db.Exists(key) {
				count++
			}
		}
		wr.WriteInteger(int64(count))
	case "TTL":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'ttl' command")
			return
		}
		ttl := h.db.TTL(args[0])
		wr.WriteInteger(int64(ttl.Seconds()))

	// --- String ---
	case "SET":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		// TODO: Parse EX/PX options
		h.db.Set(args[0], args[1], 0)
		wr.WriteSimpleString("OK")
	case "GET":
		if len(args) != 1 {
			wr.WriteError("ERR wrong number of arguments for 'get' command")
			return
		}
		val, exists := h.db.Get(args[0])
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
		h.db.MSet(pairs)
		wr.WriteSimpleString("OK")
	case "MGET":
		if len(args) == 0 {
			wr.WriteError("ERR wrong number of arguments for 'mget' command")
			return
		}
		vals := h.db.MGet(args...)
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
		val, err := h.db.Incr(args[0])
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
		val, err := h.db.Decr(args[0])
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
		count, err := h.db.LPush(args[0], stringToInterfaceSlice(args[1:])...)
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
		count, err := h.db.RPush(args[0], stringToInterfaceSlice(args[1:])...)
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
		val, err := h.db.LPop(args[0])
		if err != nil {
			wr.WriteNull() // Assuming error means empty/missing
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
		val, err := h.db.RPop(args[0])
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
		l, err := h.db.LLen(args[0])
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
		vals, err := h.db.LRange(args[0], int(start), int(stop))
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
		// HSET key field value [field value ...]
		count := 0
		for i := 1; i < len(args); i += 2 {
			n, err := h.db.HSet(args[0], args[i], args[i+1])
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
		val, err := h.db.HGet(args[0], args[1])
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
			n, err := h.db.HDel(args[0], args[i])
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
		l, err := h.db.HLen(args[0])
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
		kv, err := h.db.HGetAll(args[0])
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
		count, err := h.db.SAdd(args[0], stringToInterfaceSlice(args[1:])...)
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
		count, err := h.db.SRem(args[0], stringToInterfaceSlice(args[1:])...)
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
		members, err := h.db.SMembers(args[0])
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
		isMember, err := h.db.SIsMember(args[0], args[1])
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
		// ZADD key score member [score member ...]
		// Ledis ZAdd takes key, score, member.
		// Loop
		added := 0
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				wr.WriteError("ERR value is not a valid float")
				return
			}
			member := args[i+1]
			count, err := h.db.ZAdd(args[0], score, member)
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
		// ZRange(key, start, stop, withScores)
		res, err := h.db.ZRange(args[0], start, stop, false)
		if err != nil {
			wr.WriteArray(0)
		} else {
			wr.WriteArray(len(res))
			for _, item := range res {
				if s, ok := item.(string); ok {
					wr.WriteBulkString(s)
				} else if f, ok := item.(float64); ok {
					wr.WriteBulkString(fmt.Sprintf("%g", f)) // Use %g for float
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
		oldVal, err := h.db.SetBit(args[0], offset, int(val))
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
		val, err := h.db.GetBit(args[0], offset)
		if err != nil {
			wr.WriteInteger(0)
		} else {
			wr.WriteInteger(int64(val))
		}
	case "BITCOUNT":
		if len(args) != 1 {
			// Ignoring start/end arguments for now as our BitCount doesn't support them easily?
			// Checking ledis_bitmap.go... BitCount(key string) (uint64, error)
			// So strict 1 arg.
			if len(args) > 1 {
				wr.WriteError("ERR syntax error (arguments not supported for bitcount yet)")
				return
			}
			wr.WriteError("ERR wrong number of arguments for 'bitcount' command")
			return
		}
		count, err := h.db.BitCount(args[0])
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
		// Publish returns int64, no error
		count := h.db.Publish(args[0], args[1])
		wr.WriteInteger(count)

	// --- Stream ---
	case "XADD":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'xadd' command")
			return
		}
		// XADD key ID field value ...
		id, err := h.db.XAdd(args[0], args[1], args[2:]...)
		if err != nil {
			wr.WriteError(err.Error())
		} else {
			// Redis XADD returns the ID as bulk string usually.
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
