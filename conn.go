package gms

import (
	drv "database/sql/driver"
	"net"
)

type conn struct {
}

func newConn(c net.Conn) *conn {
	panic("unimplemented")
}

func (c *conn) Begin() (drv.Tx, error) {
	panic("unimplemented")
}

func (c *conn) Close() error {
	panic("unimplemented")
}

func (c *conn) Prepare(string) (drv.Stmt, error) {
	panic("unimplemented")
}

var (
	_ drv.Conn = (*conn)(nil)
	// _ drv.Execer  = (*conn)(nil) TODO(sanjay): implement this
	// _ drv.Queryer = (*conn)(nil) TODO(sanjay): implement this
)
