package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"sync"
)

var readerPool = sync.Pool{
	New: func() interface{} {
		return &Reader{
			reader: bufio.NewReader(nil),
		}
	},
}

var writerPool = sync.Pool{
	New: func() interface{} {
		return &Writer{
			writer: bufio.NewWriter(nil),
			buf:    make([]byte, 0, 64),
		}
	},
}

const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
	Push         = '>'
	CRLF         = "\r\n"
)

type Value struct {
	Type   byte
	IsNull bool
	Str    string
	Num    int64
	Bulk   string
	Array  []Value
}

type Reader struct {
	reader *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	r := readerPool.Get().(*Reader)
	r.reader.Reset(rd)
	return r
}

func PutReader(r *Reader) {
	r.reader.Reset(nil) // Detach from underlying reader to avoid leaks? Or just keep it.
	// Reset(nil) panics if Read is called, but that's fine.
	// Actually bufio.NewReader(nil) is fine until utilized.
	// Reset(nil) might be better to clear reference to net.Conn.
	// But bufio.Reader.Reset(io.Reader) handles nil?
	// Let's just reset when Getting.
	// But to avoid holding onto net.Conn, we should clear it.
	// How to clear bufio.Reader?
	// r.reader.Reset(nil) might work if we have a dummy reader?
	// Or we can just trust GC if we overwrite on Get?
	// But the pool keeps the *Reader which points to bufio.Reader which points to net.Conn.
	// We MUST clear the reference.
	// bufio.Reader doesn't have a Clear() method.
	// We can Reset to a dummy empty reader.
	// r.reader.Reset(emptyReader)
	readerPool.Put(r)
}

var emptyReader = &endpointReader{}

type endpointReader struct{}

func (e *endpointReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (r *Reader) ReadLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Reader) ReadInteger() (x int64, n int, err error) {
	line, n, err := r.ReadLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return i64, n, nil
}

func (r *Reader) Read() (val Value, err error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case Array:
		return r.readArray()
	case Push:
		val, err := r.readArray()
		val.Type = Push // Override type
		return val, err
	case BulkString:
		return r.readBulk()
	case SimpleString:
		return r.readSimpleString()
	case Integer:
		val.Type = Integer
		val.Num, _, err = r.ReadInteger()
		return val, err
	case Error:
		val, err = r.readSimpleString() // Reuse simple string read for error message
		val.Type = Error                // Override type to Error
		return val, err
	default:
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Reader) readArray() (val Value, err error) {
	val.Type = Array
	len, _, err := r.ReadInteger()
	if err != nil {
		return val, err
	}

	if len == -1 {
		val.IsNull = true
		return val, nil
	}

	val.Array = make([]Value, 0)
	for i := 0; i < int(len); i++ {
		v, err := r.Read()
		if err != nil {
			return val, err
		}
		val.Array = append(val.Array, v)
	}

	return val, nil
}

func (r *Reader) readBulk() (val Value, err error) {
	val.Type = BulkString
	len, _, err := r.ReadInteger()
	if err != nil {
		return val, err
	}

	if len == -1 { // Null Bulk String
		val.IsNull = true
		return val, nil
	}

	bulk := make([]byte, len)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return val, err
	}

	val.Bulk = string(bulk)

	// Read trailing CRLF
	r.ReadLine()

	return val, nil
}

func (r *Reader) readSimpleString() (val Value, err error) {
	val.Type = SimpleString
	line, _, err := r.ReadLine()
	if err != nil {
		return val, err
	}
	val.Str = string(line)
	return val, nil
}

type Writer struct {
	writer *bufio.Writer
	buf    []byte // scratch buffer for numbers
}

func NewWriter(w io.Writer) *Writer {
	wr := writerPool.Get().(*Writer)
	wr.writer.Reset(w)
	wr.buf = wr.buf[:0]
	return wr
}

func PutWriter(w *Writer) {
	w.writer.Reset(io.Discard) // prevent leak
	w.buf = w.buf[:0]
	writerPool.Put(w)
}

func (w *Writer) Flush() error {
	return w.writer.Flush()
}

func (w *Writer) Write(v Value) error {
	switch v.Type {
	case Array:
		if err := w.WriteArray(len(v.Array)); err != nil {
			return err
		}
		for _, val := range v.Array {
			if err := w.Write(val); err != nil {
				return err
			}
		}
	case BulkString:
		if v.IsNull {
			return w.WriteNull()
		}
		return w.WriteBulkString(v.Bulk)
	case SimpleString:
		return w.WriteSimpleString(v.Str)
	case Error:
		return w.WriteError(v.Str)
	case Integer:
		return w.WriteInteger(v.Num)
	default:
		return fmt.Errorf("unknown type: %v", v.Type)
	}
	return nil
}

func (w *Writer) WriteSimpleString(s string) error {
	if err := w.writer.WriteByte(SimpleString); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(s); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WriteError(s string) error {
	if err := w.writer.WriteByte(Error); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(s); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WriteInteger(i int64) error {
	if err := w.writer.WriteByte(Integer); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.buf = strconv.AppendInt(w.buf, i, 10)
	if _, err := w.writer.Write(w.buf); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WriteBulkString(s string) error {
	if err := w.writer.WriteByte(BulkString); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.buf = strconv.AppendInt(w.buf, int64(len(s)), 10)
	if _, err := w.writer.Write(w.buf); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(CRLF); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(s); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WriteNull() error {
	_, err := w.writer.WriteString("$-1\r\n")
	return err
}

func (w *Writer) WriteArray(len int) error {
	if err := w.writer.WriteByte(Array); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.buf = strconv.AppendInt(w.buf, int64(len), 10)
	if _, err := w.writer.Write(w.buf); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WritePush(len int) error {
	if err := w.writer.WriteByte(Push); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.buf = strconv.AppendInt(w.buf, int64(len), 10)
	if _, err := w.writer.Write(w.buf); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}

func (w *Writer) WriteMap(len int) error {
	if err := w.writer.WriteByte('%'); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.buf = strconv.AppendInt(w.buf, int64(len), 10)
	if _, err := w.writer.Write(w.buf); err != nil {
		return err
	}
	_, err := w.writer.WriteString(CRLF)
	return err
}
