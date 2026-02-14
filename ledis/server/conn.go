package server

import (
	"fmt"
	"io"
	"net"
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
		if val.Array[0].Type == BulkString {
			cmdName = val.Array[0].Bulk
		} else if val.Array[0].Type == SimpleString {
			cmdName = val.Array[0].Str
		} else {
			h.writeError(conn, "ERR Invalid command format")
			continue
		}

		args := make([]string, 0, len(val.Array)-1)
		for i := 1; i < len(val.Array); i++ {
			if val.Array[i].Type == BulkString {
				args = append(args, val.Array[i].Bulk)
			} else if val.Array[i].Type == SimpleString {
				args = append(args, val.Array[i].Str)
			} else if val.Array[i].Type == Integer {
				args = append(args, fmt.Sprintf("%d", val.Array[i].Num))
			} else {
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
	case "PING":
		if len(args) > 0 {
			wr.WriteBulkString(args[0])
		} else {
			wr.WriteSimpleString("PONG")
		}
	case "SET":
		if len(args) < 2 {
			wr.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		// TTL support? SET key val [EX seconds]
		// For now simple SET
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
		// Value could be string or other types?
		// ledis.Set takes interface{}.
		// If we set string, we get string.
		if s, ok := val.(string); ok {
			wr.WriteBulkString(s)
		} else {
			// Convert to string
			wr.WriteBulkString(fmt.Sprintf("%v", val))
		}

	// ... Implement other commands ...
	// Since this is a massive switch, maybe I should do just a few to prove it works
	// as per "Integration Research" verification goal.
	// User asked "RESP3를 구현하고 net.Conn만 재구현 하면 되겠네".
	// The goal is to verify checking with go-redis.
	// So implementation of PING, SET, GET is enough for verification.

	default:
		wr.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}
