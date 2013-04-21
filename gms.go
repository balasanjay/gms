package gms

import (
	"database/sql"
	drv "database/sql/driver"
)

type driver struct {
}

func (d *driver) Open(dsn string) (drv.Conn, error) {
	return nil, nil
}

func init() {
	sql.Register("gms", &driver{})
}
