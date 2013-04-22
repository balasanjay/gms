package gms_test

import (
	"database/sql"
	_ "github.com/balasanjay/gms"
	"testing"
	"time"
)

func TestSimple(t *testing.T) {
	before := time.Now()
	db, err := sql.Open("gms", "tcp://root:@localhost:3306?db=test&timeout=1s")
	if err != nil {
		t.Errorf("sql.Open error: %v", err)
		return
	}

	t.Logf("sql.Open took %v", time.Since(before))

	_, err = db.Exec("CREATE TABLE test (value BOOL);")
	_, err = db.Exec("SELECT * FROM manualtest WHERE value = ?;")
	// _, err = db.Exec("SELECT * FROM manualtest WHERE value = ?;", 3)
	if err != nil {
		t.Errorf("db.Exec error: %v", err)
		return
	}
}
