package gms

import (
	drv "database/sql/driver"
)

type stmt struct {
}

func (s *stmt) Close() error {
	panic("unimplemented")
}

func (s *stmt) Exec([]drv.Value) (drv.Result, error) {
	panic("unimplemented")
}

func (s *stmt) NumInput() int {
	panic("unimplemented")
}

func (s *stmt) Query([]drv.Value) (drv.Rows, error) {
	panic("unimplemented")
}

var _ drv.Stmt = (*stmt)(nil)
