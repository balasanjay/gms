package gms

import (
	drv "database/sql/driver"
	"errors"
)

type stmt struct {
	// The backing connection
	c *conn

	// The id of this statement, as assigned by the MySQL server
	id uint32

	// Descriptors for the input and output fields respectively. In MySQL
	// parlance, these are the params and the columns respectively.
	inputFields  []field
	outputFields []field
}

func (s *stmt) Close() error {
	panic("unimplemented")
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

	if c.scratch[0] != 0 {
		// TODO(sanjay): explain this and retrieve the rest of the info
		// from the connection
		return nil, errors.New("exec failed")
	}

	affRows, err := c.ReadLengthEncodedInt()
	if err != nil {
		return nil, err
	}

	lastInsertId, err := c.ReadLengthEncodedInt()
	if err != nil {
		return nil, err
	}

	return results{affectedRows: int64(affRows), lastInsertId: int64(lastInsertId)}, nil
}

type results struct {
	affectedRows int64
	lastInsertId int64
}

func (r results) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r results) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

func (s *stmt) NumInput() int {
	return len(s.inputFields)
}

func (s *stmt) Query([]drv.Value) (drv.Rows, error) {
	panic("unimplemented")
}

func (s *stmt) sendQuery(params []drv.Value) error {
	if len(s.inputFields) != len(params) {
		return errors.New("field count mismatch")
	}

	panic("uncompleted")
	return nil
}

var _ drv.Stmt = (*stmt)(nil)
