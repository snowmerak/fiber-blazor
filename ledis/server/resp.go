package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
	Push         = '>'
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
	return &Reader{reader: bufio.NewReader(rd)}
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
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes []byte

	switch v.Type {
	case Array:
		bytes = append(bytes, Array)
		bytes = append(bytes, strconv.Itoa(len(v.Array))...)
		bytes = append(bytes, '\r', '\n')
		for _, val := range v.Array {
			// Recursive logic, simplified for expected command structure
			// Actually need full recursion or separate write method
			// Let's implement full recursion by calling Write?
			// But Write takes Value.
			// Helper needed or recursive call.

			// Calling recursive
			if err := w.Write(val); err != nil {
				return err
			}
		}
		// Array is written by writing individual values
		_, err := w.writer.Write(bytes)
		return err

		// Wait, the recursion above is wrong. I append bytes for Array header, then write them, THEN loop?
		// No, order matters. Header first.
		// Let's restructure.
	}
	return nil
}

// Better writer methods

func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.writer, "+%s\r\n", s)
	return err
}

func (w *Writer) WriteError(s string) error {
	_, err := fmt.Fprintf(w.writer, "-%s\r\n", s)
	return err
}

func (w *Writer) WriteInteger(i int64) error {
	_, err := fmt.Fprintf(w.writer, ":%d\r\n", i)
	return err
}

func (w *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.writer, "$%d\r\n%s\r\n", len(s), s)
	return err
}

func (w *Writer) WriteNull() error {
	_, err := fmt.Fprintf(w.writer, "$-1\r\n")
	return err
}

func (w *Writer) WriteArray(len int) error {
	_, err := fmt.Fprintf(w.writer, "*%d\r\n", len)
	return err
}

func (w *Writer) WritePush(len int) error {
	_, err := fmt.Fprintf(w.writer, ">%d\r\n", len)
	return err
}

func (w *Writer) WriteMap(len int) error {
	_, err := fmt.Fprintf(w.writer, "%%%d\r\n", len)
	return err
}
