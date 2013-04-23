package gms

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
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

func (c *conn) ReadLengthEncodedInt() (uint64, error) {
	err := readExactly(c, c.scratch[:1])
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

	err = readExactly(c, c.scratch[:intSize])
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
	strSize, err := c.ReadLengthEncodedInt()
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

	nw, err := c.Write(c.scratch[0:size])
	if err != nil {
		return 0, err
	} else if nw != size {
		return 0, io.ErrShortWrite
	}

	return nw, nil
}
