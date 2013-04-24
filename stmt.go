package gms

import (
	drv "database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"time"
)

type inputFieldData struct {
	field
}

type outputFieldData struct {
	field

	// The following fields are used when parsing a column from the binary
	// wire protocol.

	isNull bool

	// If bufEndIdx != -1, then this field's data is inside the connection's
	// reusable buffer, with the last byte of this field's data being at
	// bufEndIdx-1. We do it this way instead of storing a slice, so that if the
	// buffer resizes when we are writing to it, we don't capture any of the old
	// slices, and keep them alive for longer than necessary.
	bufEndIdx int
}

type stmt struct {
	// The backing connection
	c *conn

	// The id of this statement, as assigned by the MySQL server
	id uint32

	// Descriptors for the input and output fields respectively. In MySQL
	// parlance, these are the params and the columns respectively.
	inputFields  []inputFieldData
	outputFields []outputFieldData
}

func (s *stmt) Close() error {
	c := s.c
	c.seqId = 0

	c.scratch[0] = comStmtClose
	binary.LittleEndian.PutUint32(c.scratch[1:5], s.id)

	c.BeginPacket(5)

	_, err := c.Write(c.scratch[:5])
	if err != nil {
		return err
	}

	err = c.EndPacket(FLUSH)
	if err != nil {
		return err
	}

	s.c = nil
	s.inputFields = nil
	s.outputFields = nil

	return nil
}

func (s *stmt) Exec(params []drv.Value) (drv.Result, error) {
	err := s.sendQuery(params)
	if err != nil {
		return nil, err
	}

	c := s.c
	err = c.AdvancePacket()
	if err != nil {
		return nil, err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return nil, err
	}

	if c.scratch[0] == 0xff {
		// This is an error packet
		return nil, c.ErrorFromErrPacket()
	} else if c.scratch[0] != 0x00 {
		// This query has result rows. The user is not interested in these, so
		// we simply skip over them until we find 2 seperate EOF packets.
		for i := 0; i < 2; i++ {
			err = c.SkipPacketsUntilEOFPacket()
			if err != nil {
				return nil, err
			}
		}
		return unknownResults(0), nil
	}

	// Otherwise, this is an OK packet, and we can simply pull out the number
	// of affected rows and last insert id.

	affRows, err := c.ReadLengthEncodedInt(c)
	if err != nil {
		return nil, err
	}

	lastInsertId, err := c.ReadLengthEncodedInt(c)
	if err != nil {
		return nil, err
	}

	return results{affectedRows: int64(affRows), lastInsertId: int64(lastInsertId)}, nil
}

func (s *stmt) NumInput() int {
	return len(s.inputFields)
}

func (s *stmt) Query(args []drv.Value) (drv.Rows, error) {
	err := s.sendQuery(args)
	if err != nil {
		return nil, err
	}

	c := s.c
	err = c.AdvancePacket()
	if err != nil {
		return nil, err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return nil, err
	}

	if c.scratch[0] == 0xff {
		// This is an error packet
		return nil, c.ErrorFromErrPacket()
	} else if c.scratch[0] == 0x00 {
		// This is an OK packet, meaning no rows were there to be read.
		err = c.AdvanceToEOF()
		if err != nil {
			return nil, err
		}

		return &resultIter{
			atEOF: true,
			c:     c,
			s:     s,
		}, nil
	}

	// We don't need to read the column definitions because we parsed them when
	// we prepared the statement.
	err = c.SkipPacketsUntilEOFPacket()
	if err != nil {
		return nil, err
	}

	return &resultIter{c: c, s: s}, nil
}

func (s *stmt) sendQuery(params []drv.Value) error {
	if len(s.inputFields) != len(params) {
		return errors.New("field count mismatch")
	}

	c := s.c
	c.seqId = 0

	// First, we need to compute the size of the packet we will need
	size := int64(1) + // command byte
		4 + // statement id
		1 + // flags
		4 // iteration count

	if numInputs := int64(len(s.inputFields)); numInputs > 0 {
		size += (numInputs + 7) / 8 // NULL bitmap
		size += 1                   // new-params-bound
		size += numInputs * 2       // types

		for i := range params {
			if params[i] == nil {
				continue
			}

			paramSize, _, err := s.WriteObj(globalCountingWriter, params[i])
			if err != nil {
				return err
			}

			size += int64(paramSize)
		}
	}

	// Now that we've completed computing the size of the packet, we begin
	// the actual content of the packet
	c.BeginPacket(size)

	c.scratch[0] = comStmtExecute
	binary.LittleEndian.PutUint32(c.scratch[1:5], s.id)
	c.scratch[5] = 0
	c.scratch[6] = 0x01
	c.scratch[7] = 0x00
	c.scratch[8] = 0x00
	c.scratch[9] = 0x00

	_, err := c.Write(c.scratch[:10])
	if err != nil {
		return err
	}

	if len(params) <= 0 {
		err = c.EndPacket(FLUSH)
		if err != nil {
			return err
		}
		return nil
	}

	for i := uint(0); i < (uint(len(params))+7)/8; i++ {
		c.scratch[0] = 0
		for j := uint(0); j < 8; j++ {
			idx := i*8 + j
			if idx >= uint(len(params)) {
				break
			}

			if params[idx] == nil {
				c.scratch[0] |= 1 << (idx % 8)
			}
		}
		_, err = c.Write(c.scratch[:1])
		if err != nil {
			return err
		}
	}

	// new-params-bound == 1
	c.scratch[0] = 1
	_, err = c.Write(c.scratch[:1])
	if err != nil {
		return err
	}

	// Types
	for i := range params {
		var ftype fieldType

		if params[i] != nil {
			_, ftype, err = s.WriteObj(ioutil.Discard, params[i])
			if err != nil {
				return err
			}
		} else {
			ftype = 0
		}

		c.scratch[0] = byte(ftype)
		c.scratch[1] = 0
		_, err = c.Write(c.scratch[:2])
		if err != nil {
			return err
		}
	}

	// Values
	for i := range params {
		if params[i] == nil {
			continue
		}

		_, _, err = s.WriteObj(c, params[i])
		if err != nil {
			return err
		}
	}

	err = c.EndPacket(FLUSH)
	if err != nil {
		return err
	}

	return nil
}

var globalCountingWriter io.Writer = countingWriter(true)

type countingWriter bool

func (c countingWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (s *stmt) WriteObj(w io.Writer, arg drv.Value) (int, fieldType, error) {
	c := s.c
	switch v := arg.(type) {
	case int64:
		binary.LittleEndian.PutUint64(c.scratch[0:8], uint64(v))
		_, err := w.Write(c.scratch[:8])
		return 8, fieldTypeLongLong, err
	case float64:
		binary.LittleEndian.PutUint64(c.scratch[0:8], uint64(math.Float64bits(v)))
		_, err := w.Write(c.scratch[:8])
		return 8, fieldTypeDouble, err
	case bool:
		if v {
			c.scratch[0] = 1
		} else {
			c.scratch[0] = 0
		}
		_, err := w.Write(c.scratch[:1])
		return 1, fieldTypeTiny, err
	case []byte:
		n, err := c.WriteLengthEncodedInt(w, uint64(len(v)))
		if err != nil {
			return 0, fieldTypeString, err
		}

		n2, err := w.Write(v)
		if err != nil {
			return 0, fieldTypeString, err
		}
		return n + n2, fieldTypeString, nil
	case string:
		n, err := c.WriteLengthEncodedInt(w, uint64(len(v)))
		if err != nil {
			return 0, fieldTypeString, err
		}

		n2, err := io.WriteString(w, v)
		if err != nil {
			return 0, fieldTypeString, err
		}
		return n + n2, fieldTypeString, nil
	case time.Time:
		size := 0

		binary.LittleEndian.PutUint16(c.scratch[1:3], uint16(v.Year()))
		c.scratch[3] = byte(v.Month() - 1)
		c.scratch[4] = byte(v.Day())
		c.scratch[5] = byte(v.Hour())
		c.scratch[6] = byte(v.Minute())
		c.scratch[7] = byte(v.Second())
		binary.LittleEndian.PutUint32(c.scratch[8:12], uint32(v.Nanosecond()/int(time.Microsecond)))

		if v.Nanosecond()/int(time.Microsecond) != 0 {
			size = 12
		} else if v.Second() != 0 || v.Minute() != 0 || v.Hour() != 0 {
			size = 8
		} else if v.Year() != 0 || v.Month() != 1 || v.Day() != 0 {
			size = 5
		} else {
			size = 1
		}
		c.scratch[0] = byte(size - 1)

		n, err := w.Write(c.scratch[:size])
		return n, fieldTypeTimestamp, err
	default:
		break
	}

	return 0, 0, fmt.Errorf("Can't convert type: %T", arg)
}

var _ drv.Stmt = (*stmt)(nil)
