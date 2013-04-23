package gms

import (
	drv "database/sql/driver"
	"io"
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

	panic("incomplete")
}
