package gms

import (
	"database/sql"
	drv "database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"time"
)

type UnknownProtocolError struct {
	prot string
}

func (u *UnknownProtocolError) Error() string {
	return fmt.Sprintf("unknown protocol: %q", u.prot)
}

type driver struct {
}

func (d *driver) Open(dsn string) (drv.Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	params := u.Query()

	var (
		username = ""
		password = ""
		db       = ""
	)

	if u.User != nil {
		username = u.User.Username()
		if tmp, ok := u.User.Password(); ok {
			password = tmp
		}
	}

	if tmp := params.Get("db"); tmp != "" {
		db = tmp
	}

	var (
		dialer net.Dialer
	)

	if tmp, err := time.ParseDuration(params.Get("timeout")); err == nil {
		dialer.Timeout = tmp
	}

	var (
		prot string
		addr string
	)

	prot = u.Scheme
	switch prot {
	case "tcp":
		addr = u.Host
	case "unix":
		addr = u.Path
	default:
		return nil, &UnknownProtocolError{prot: prot}
	}

	nc, err := dialer.Dial(prot, addr)
	if err != nil {
		return nil, err
	}

	c := newConn(nc)

	// We have to complete the handshake before we can use the connection.
	err = c.handshake(username, password, db)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func init() {
	sql.Register("gms", &driver{})
}
