package gms_test

import (
	"database/sql"
	_ "github.com/balasanjay/gms"
	"testing"
)

func TestSimple(t *testing.T) {
	db, err := sql.Open("gms", "tcp://root:asdas@localhost:3306?db=test&timeout=1s")
	if err != nil {
		t.Errorf("sql.Open error: %v", err)
		return
	}

	_, err = db.Exec("CREATE TABLE test (value BOOL)")
	if err != nil {
		t.Errorf("db.Exec error: %v", err)
		return
	}
}
