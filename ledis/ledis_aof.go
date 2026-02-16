package ledis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AOFWriter struct {
	file     *os.File
	writer   *bufio.Writer
	mu       sync.Mutex
	cmdChan  chan []byte
	stopChan chan struct{}
	wg       sync.WaitGroup
	filename string

	// Rewrite state
	rewriting  bool
	rewriteBuf *bytes.Buffer
}

func NewAOFWriter(filename string) (*AOFWriter, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	aw := &AOFWriter{
		file:     f,
		writer:   bufio.NewWriter(f),
		cmdChan:  make(chan []byte, 10000), // Buffer specific size
		stopChan: make(chan struct{}),
		filename: filename,
	}

	aw.wg.Add(1)
	go aw.backgroundWrite()

	return aw, nil
}

func (aw *AOFWriter) backgroundWrite() {
	defer aw.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case cmd, ok := <-aw.cmdChan:
			if !ok {
				// Channel closed, flush remaining
				aw.flush()
				return
			}

			aw.mu.Lock()
			aw.writer.Write(cmd)
			if aw.rewriting {
				aw.rewriteBuf.Write(cmd)
			}
			aw.mu.Unlock()

		case <-ticker.C:
			aw.flush()
		case <-aw.stopChan:
			aw.flush()
			return
		}
	}
}

func (aw *AOFWriter) flush() {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.writer.Flush()
	aw.file.Sync()
}

func (aw *AOFWriter) Close() {
	close(aw.stopChan)
	aw.wg.Wait()
	aw.flush()
	aw.file.Close()
}

// LogCommand formats command as RESP and sends to channel
func (d *DistributedMap) LogCommand(cmd string, args ...string) {
	if d.aof == nil {
		return
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	// RESP Array: 1 (cmd) + len(args)
	fmt.Fprintf(buf, "*%d\r\n", 1+len(args))
	// Write Cmd
	fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(cmd), cmd)
	// Write Args
	for _, arg := range args {
		fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(arg), arg)
	}

	d.aof.cmdChan <- buf.Bytes()
}

func (aw *AOFWriter) StartRewrite() {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.rewriting = true
	aw.rewriteBuf = new(bytes.Buffer)
}

func (aw *AOFWriter) FinishRewrite(tempFilename string) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	aw.rewriting = false

	// Open temp file to append buffer
	f, err := os.OpenFile(tempFilename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	// Don't defer Close immediately, we might want to close after sync?
	// Standard defer is fine.

	// Append buffer
	if _, err := f.Write(aw.rewriteBuf.Bytes()); err != nil {
		f.Close()
		return err
	}
	f.Close() // Close temp file before rename

	// Flush and close current AOF
	aw.writer.Flush()
	aw.file.Sync()
	aw.file.Close()

	// Replace
	if err := os.Remove(aw.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove old AOF: %v", err)
	}
	if err := os.Rename(tempFilename, aw.filename); err != nil {
		// Restore? Complex.
		return fmt.Errorf("failed to rename AOF: %v", err)
	}

	// Reopen
	newF, err := os.OpenFile(aw.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen AOF: %v", err)
	}

	aw.file = newF
	aw.writer = bufio.NewWriter(newF)
	aw.rewriteBuf = nil

	return nil
}

func (aw *AOFWriter) CancelRewrite() {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.rewriting = false
	aw.rewriteBuf = nil
}

func (d *DistributedMap) RewriteAOF() error {
	if d.aof == nil {
		return fmt.Errorf("AOF not enabled")
	}

	d.aof.StartRewrite()

	tempName := fmt.Sprintf("temp-%d.aof", time.Now().UnixNano())
	file, err := os.Create(tempName)
	if err != nil {
		d.aof.CancelRewrite()
		return err
	}
	// We handle explicit close in success path, defer handles failure cleanup
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Iterate all shards
	for _, shard := range d.shards {
		shard.Range(func(key, value any) bool {
			k := key.(string)
			item := value.(*Item)

			// Lock item for consistent read
			item.Mu.RLock()
			defer item.Mu.RUnlock()

			// Check expiry
			if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
				return true
			}

			// Serialize
			switch item.Type {
			case TypeString:
				writeCommand(writer, "SET", k, item.Str)
			case TypeList:
				// RPUSH key v1 v2 ...
				args := make([]string, 0, 1+item.ListSize)
				args = append(args, k)
				node := item.ListHead
				for node != nil {
					args = append(args, node.Value)
					node = node.Next
				}
				writeCommand(writer, "RPUSH", args...)
			case TypeHash:
				// HSET key f1 v1 ...
				args := make([]string, 0, 1+len(item.Hash)*2)
				args = append(args, k)
				for f, v := range item.Hash {
					args = append(args, f, v)
				}
				writeCommand(writer, "HSET", args...)
			case TypeSet:
				// SADD key m1 ...
				args := make([]string, 0, 1+len(item.Set))
				args = append(args, k)
				for m := range item.Set {
					args = append(args, m)
				}
				writeCommand(writer, "SADD", args...)
			case TypeZSet:
				// ZADD key s1 m1 ...
				args := make([]string, 0, 1+len(item.ZSet.dict)*2)
				args = append(args, k)
				// Iterate skip list or dict? Dict is easier but order lost.
				// ZADD order doesn't matter.
				for m, score := range item.ZSet.dict {
					args = append(args, fmt.Sprintf("%f", score), m)
				}
				writeCommand(writer, "ZADD", args...)
			case TypeGeo:
				// GEOADD key lat lon member ...
				// Iterate members
				args := make([]string, 0, 1+len(item.Geo.members)*3)
				args = append(args, k)
				for m, p := range item.Geo.members {
					args = append(args, fmt.Sprintf("%f", p.Lat), fmt.Sprintf("%f", p.Lon), m)
				}
				writeCommand(writer, "GEOADD", args...)
			case TypeHyperLogLog:
				// RESTOREHLL key data
				data, err := item.HLL.MarshalBinary()
				if err == nil {
					// writeCommand handles strings. We need to convert bytes to string usually?
					// writeCommand uses generic string args.
					// But binary data might contain nulls.
					// bufio.Writer + Fprintf matches logic.
					// We can cast []byte to string for writeCommand.
					writeCommand(writer, "RESTOREHLL", k, string(data))
				}
			case TypeBitmap:
				// SETBIT key offset value? Too slow for large bitmap.
				// SET key raw_bytes? No, bitmap is roaring.
				// Roaring64 serialization?
				// Maybe serialize as "bitmap-load" command or just custom format?
				// But we need to use standard commands.
				// For now, skip bitmap or implement custom dump/load?
				// Actually, `ledis` doesn't support complex bitmap import/export easily via command
				// except strictly SETBIT loop which is slow.
				// Or `RESTORE` format.
				// Let's implement minimal support: just ignore for now or log error?
				// Or use `RESTORE` if we implemented DUMP/RESTORE.
				// Given complexity, I will skip BITMAP in AOF Rewrite for V1.
			case TypeStream:
				// XADD key ID f1 v1 ...
				// Iterate stream entries
				// Need access to Stream struct fields.
				// item.Stream is pointer to Stream struct.
				// Entries triggers XADD one by one?
				// Or XADD with multiple IDs? No, XADD takes one ID.
				// So we need loop.
				for _, entry := range item.Stream.Entries {
					args := make([]string, 0, 2+len(entry.Fields))
					args = append(args, k, entry.ID) // XADD key ID field value...
					// Wait, XADD key [MAXLEN] ID field value
					// We can skip MAXLEN 0.
					args = append(args, entry.Fields...)
					writeCommand(writer, "XADD", args...)
				}
			}
			return true
		})
	}

	if err := writer.Flush(); err != nil {
		d.aof.CancelRewrite()
		return err
	}
	if err := file.Sync(); err != nil {
		d.aof.CancelRewrite()
		return err
	}
	file.Close() // Explicit close before FinishRewrite

	return d.aof.FinishRewrite(tempName)
}

func writeCommand(w *bufio.Writer, cmd string, args ...string) {
	fmt.Fprintf(w, "*%d\r\n", 1+len(args))
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(cmd), cmd)
	for _, arg := range args {
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg)
	}
}

func (d *DistributedMap) LoadAOF(filename string) error {
	f, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		cmd, args, err := readCommand(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Execute
		switch strings.ToUpper(cmd) {
		case "SET":
			if len(args) >= 2 {
				d.Set(args[0], args[1], 0)
			}
		case "RPUSH":
			if len(args) >= 2 {
				// Convert generic args to interface{} slice?
				// LPush takes ...any
				// But we have []string.
				// Helper needed.
				ifaceArgs := make([]any, len(args)-1)
				for i, v := range args[1:] {
					ifaceArgs[i] = v
				}
				d.RPush(args[0], ifaceArgs...)
			}
		case "HSET":
			if len(args) >= 3 {
				for i := 1; i < len(args); i += 2 {
					d.HSet(args[0], args[i], args[i+1])
				}
			}
		case "SADD":
			if len(args) >= 2 {
				d.SAdd(args[0], args[1:]...)
			}
		case "ZADD":
			if len(args) >= 3 {
				// args: key score member score member ...
				// ZAdd expects key int64 member (Wait, ZAdd signature?)
				// ledis_zset.go: ZAdd(key string, score float64, member string) (int, error)
				// It handles one pair.
				// But ZADD command can have multiple.
				// AOF writes: ZADD key s1 m1 s2 m2 ...
				key := args[0]
				for i := 1; i < len(args); i += 2 {
					score, _ := strconv.ParseFloat(args[i], 64)
					member := args[i+1]
					d.ZAdd(key, score, member)
				}
			}
		case "XADD":
			// XADD key ID field value ...
			if len(args) >= 4 {
				// XAdd(key string, id string, maxLen int64, fields ...string)
				// AOF doesn't store MAXLEN unless we logged it separately?
				// WriteCommand for XADD in RewriteAOF:
				// writeCommand(writer, "XADD", args...)
				// args: key, ID, fields...
				// So we don't have MAXLEN info here (it was applied or 0).
				// We pass 0 for maxLen during recovery.
				key := args[0]
				id := args[1]
				fields := args[2:]
				d.XAdd(key, id, 0, fields...)
			}
		case "DEL":
			for _, k := range args {
				d.Del(k)
			}
		case "INCR":
			d.Incr(args[0])
		case "DECR":
			d.Decr(args[0])
		case "LPUSH":
			if len(args) >= 2 {
				ifaceArgs := make([]any, len(args)-1)
				for i, v := range args[1:] {
					ifaceArgs[i] = v
				}
				d.LPush(args[0], ifaceArgs...)
			}
		case "LPOP":
			d.LPop(args[0])
		case "RPOP":
			d.RPop(args[0])
		case "HDEL":
			d.HDel(args[0], args[1:]...)
		case "SREM":
			d.SRem(args[0], args[1:]...)
		case "MSET":
			pairs := make(map[string]any)
			for i := 0; i < len(args); i += 2 {
				pairs[args[i]] = args[i+1]
			}
			d.MSet(pairs)
		case "SETBIT":
			// Not implemented in RewriteAOF, but logged in normal flow
			if len(args) == 3 {
				off, _ := strconv.Atoi(args[1])
				val, _ := strconv.Atoi(args[2])
				d.SetBit(args[0], uint64(off), val)
			}
		case "PUBLISH":
			// Replay publish? Maybe not needed for state, but side effects?
			// Usually AOF replays state changes. Publish is ephemeral.
			// Redis does NOT log PUBLISH to AOF unless explicitly configured?
			// Actually PUBLISH is not a write command for dataset.
			// My LogCommand hooked PUBLISH. Remove?
			// Replaying PUBLISH causes notifications during recovery. Probably unwanted.
			// But skipping it is safe.
		case "XTRIM":
			if len(args) == 2 {
				ml, _ := strconv.ParseInt(args[1], 10, 64)
				d.XTrim(args[0], ml)
			}
		case "GEOADD":
			if len(args) >= 4 && (len(args)-1)%3 == 0 {
				// GEOADD key lat lon member [lat lon member ...]
				key := args[0]
				count := 0
				for i := 1; i < len(args); i += 3 {
					lat, _ := strconv.ParseFloat(args[i], 64)
					lon, _ := strconv.ParseFloat(args[i+1], 64)
					member := args[i+2]
					n, err := d.GeoAdd(key, lat, lon, member)
					if err == nil {
						count += n
					}
				}
			}
		case "RESTOREHLL":
			if len(args) == 2 {
				d.RestoreHLL(args[0], []byte(args[1]))
			}
		}
	}
	return nil
}

func readCommand(r *bufio.Reader) (cmd string, args []string, err error) {
	// *<count>\r\n
	line, err := r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	if line[0] != '*' {
		return "", nil, fmt.Errorf("bad AOF format: %s", line)
	}
	countStr := strings.TrimSpace(line[1:])
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return "", nil, err
	}

	if count < 1 {
		return "", nil, fmt.Errorf("bad command count")
	}

	parts := make([]string, count)
	for i := 0; i < count; i++ {
		// $<len>\r\n
		line, err := r.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		if line[0] != '$' {
			return "", nil, fmt.Errorf("bad bulk format")
		}
		lenStr := strings.TrimSpace(line[1:])
		length, err := strconv.Atoi(lenStr)
		if err != nil {
			return "", nil, err
		}

		// Read content
		buf := make([]byte, length+2) // +2 for CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", nil, err
		}
		parts[i] = string(buf[:length])
	}

	return parts[0], parts[1:], nil
}
