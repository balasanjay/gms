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

// This file implements utilities for reading and writing stream values

func readExactly(r io.Reader, buf []byte) error {
	n, err := io.ReadAtLeast(r, buf, len(buf))
	if err == io.EOF || n != len(buf) {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}
	return nil
}

func (c *conn) ReadLengthEncodedInt(r io.Reader) (uint64, error) {
	err := readExactly(r, c.scratch[:1])
	if err != nil {
		return 0, err
	}

	if c.scratch[0] < 0xfb {
		return uint64(c.scratch[0]), nil
	}

	intSize := uint64(0)
	switch c.scratch[0] {
	case 0xfc:
		intSize = 2
	case 0xfd:
		intSize = 3
	case 0xfe:
		intSize = 8
	default:
		return 0, errors.New("unknown length encoded integer")
	}

	err = readExactly(r, c.scratch[:intSize])
	if err != nil {
		return 0, err
	}

	ret := uint64(0)
	for i := uint64(0); i < intSize; i++ {
		ret |= uint64(c.scratch[i]) << (8 * i)
	}

	return ret, nil
}

func (c *conn) SkipLengthEncodedString() error {
	strSize, err := c.ReadLengthEncodedInt(c)
	if err != nil {
		return err
	}
	_, err = io.CopyN(ioutil.Discard, c, int64(strSize))
	if err != nil {
		return err
	}

	return nil
}

func (c *conn) WriteLengthEncodedInt(w io.Writer, n uint64) (int, error) {
	size := 0
	if n < 251 {
		size = 1
		c.scratch[0] = byte(n)
	} else if n < (1 << 16) {
		size = 3
		c.scratch[0] = 0xfc
		binary.LittleEndian.PutUint16(c.scratch[1:3], uint16(n))
	} else if n < (1 << 24) {
		size = 4
		c.scratch[0] = 0xfd
		c.scratch[1] = byte(n)
		c.scratch[2] = byte(n >> 8)
		c.scratch[3] = byte(n >> 16)
	} else {
		size = 9
		c.scratch[0] = 0xfe
		binary.LittleEndian.PutUint64(c.scratch[1:9], n)
	}

	nw, err := w.Write(c.scratch[0:size])
	if err != nil {
		return 0, err
	} else if nw != size {
		return 0, io.ErrShortWrite
	}

	return nw, nil
}

func (c *conn) ReadValue(o *outputFieldData, dst *drv.Value) error {
	unsigned := (o.flag & flagUnsigned) != 0 // TODO(sanjay): test this...
	switch o.ftype {
	case fieldTypeNULL:
		*dst = nil
		o.bufEndIdx = -1
		return nil
	case fieldTypeTiny:
		err := readExactly(c, c.scratch[:1])
		if err != nil {
			return err
		}
		if unsigned {
			*dst = int64(c.scratch[0])
		} else {
			*dst = int64(int8(c.scratch[0]))
		}
		o.bufEndIdx = -1
		return nil
	case fieldTypeShort, fieldTypeYear:
		err := readExactly(c, c.scratch[:2])
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint16(c.scratch[:2])
		if unsigned {
			*dst = int64(val)
		} else {
			*dst = int64(int16(val))
		}
		o.bufEndIdx = -1
		return nil
	case fieldTypeInt24, fieldTypeLong, fieldTypeFloat:
		err := readExactly(c, c.scratch[:4])
		if err != nil {
			return err
		}

		val := binary.LittleEndian.Uint32(c.scratch[:4])
		if o.ftype == fieldTypeFloat {
			*dst = float64(math.Float32frombits(val))
		} else if unsigned {
			*dst = int64(val)
		} else {
			*dst = int64(int32(val))
		}
		o.bufEndIdx = -1
		return nil
	case fieldTypeLongLong, fieldTypeDouble:
		err := readExactly(c, c.scratch[:8])
		if err != nil {
			return err
		}

		// TODO(sanjay): test this for unsigned and signed values
		val := binary.LittleEndian.Uint64(c.scratch[:8])
		if o.ftype == fieldTypeDouble {
			*dst = math.Float64frombits(val)
		} else {
			*dst = int64(val)
		}
		o.bufEndIdx = -1
		return nil

	// Length coded Binary Strings
	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString:
		length, err := c.ReadLengthEncodedInt(c)
		// TODO(sanjay): the other client handles a NULL value here, look into
		// when this would come up.
		if err != nil {
			return err
		}

		// TODO(sanjay): if this uint64->int[64] conversion results in a negative
		// number, we will have bad times.
		c.reuseBuf.Grow(int(length))
		_, err = io.CopyN(c.reuseBuf, c, int64(length))
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} else if err != nil {
			return err
		}
		o.bufEndIdx = c.reuseBuf.Len()
		return nil
	case fieldTypeDate, fieldTypeDateTime, fieldTypeTimestamp, fieldTypeNewDate:
		err := readExactly(c, c.scratch[:1])
		if err != nil {
			return err
		}

		size := c.scratch[0]

		var (
			year        = 0
			month       = 0
			day         = 0
			hour        = 0
			minute      = 0
			second      = 0
			microsecond = 0
		)

		if size >= 4 {
			err = readExactly(c, c.scratch[:4])
			if err != nil {
				return err
			}

			year = int(binary.LittleEndian.Uint16(c.scratch[:2]))
			month = int(c.scratch[2])
			day = int(c.scratch[3])
		}
		if size >= 7 {
			err = readExactly(c, c.scratch[:3])
			if err != nil {
				return err
			}

			hour = int(c.scratch[0])
			minute = int(c.scratch[1])
			second = int(c.scratch[2])
		}
		if size >= 11 {
			err = readExactly(c, c.scratch[:4])
			if err != nil {
				return err
			}

			microsecond = int(binary.LittleEndian.Uint32(c.scratch[:4]))
		}

		*dst = time.Date(year, time.Month(month+1), day, hour, minute, second, microsecond*1e3, time.UTC)
		o.bufEndIdx = -1
		return nil
	default:
		return fmt.Errorf("Cannot read field type %x", o.ftype)
	}
	panic("unreachable")
}
