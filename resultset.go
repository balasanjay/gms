package gms

import (
	drv "database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
)

type resultIter struct {
	atEOF bool
	c     *conn
	s     *stmt
}

func (r *resultIter) Close() error {
	if r.atEOF {
		return nil
	}

	err := r.c.SkipPacketsUntilEOFPacket()
	if err != nil {
		return err
	}

	r.atEOF = true
	r.c = nil
	r.s = nil
	return nil
}

func (r *resultIter) Columns() []string {
	ret := make([]string, 0, len(r.s.outputFields))
	for i := range r.s.outputFields {
		ret = append(ret, r.s.outputFields[i].name)
	}
	return ret
}

func (r *resultIter) Next(dest []drv.Value) error {
	if r.atEOF {
		return io.EOF
	}

	c := r.c
	s := r.s

	err := c.AdvancePacket()
	if err != nil {
		return err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return err
	}

	// If we read an EOF packet, then record that fact, skip the rest of the
	// packet, and return io.EOF.
	if c.scratch[0] == 0xfe && c.lr.N <= 4 {
		r.atEOF = true
		err = c.AdvanceToEOF()
		if err != nil {
			return err
		}
		return io.EOF
	}

	if c.scratch[0] != 0x00 {
		// TODO(sanjay): fix this panic
		panic("unexpected first byte of binary result set row")
	}

	// Otherwise, we've reached a data packet. First, deal with the NULL bitmap.
	curBitmapByte := -1
	const offset = 2
	for i := range s.outputFields {
		f := &s.outputFields[i]
		thisBitmapByte := (i + offset) / 8
		if thisBitmapByte != curBitmapByte {
			err = readExactly(c, c.scratch[:1])
			if err != nil {
				return err
			}
			curBitmapByte = thisBitmapByte
		}

		bitIdx := uint((i + offset) % 8)
		f.isNull = (c.scratch[0] & byte(1<<bitIdx)) != 0
	}

	c.reuseBuf.Reset()
	for i := range s.outputFields {
		f := &s.outputFields[i]
		if f.isNull {
			dest[i] = nil
			continue
		}
		err = c.ReadValue(f, &dest[i])
		if err != nil {
			return err
		}
	}

	bufStartIdx := 0
	buf := c.reuseBuf.Bytes()
	for i := range s.outputFields {
		f := &s.outputFields[i]
		if f.isNull {
			continue
		}
		if f.bufEndIdx == -1 {
			continue
		}
		dest[i] = buf[bufStartIdx:f.bufEndIdx]
		bufStartIdx = f.bufEndIdx
	}

	// Sanity-check that we've exhausted a packet
	if c.lr.N != 0 {
		_, err = io.Copy(ioutil.Discard, c)
		if err != nil {
			return err
		}
		return fmt.Errorf("data packet has %d more bytes than expected", c.lr.N)
	}

	return nil
}
