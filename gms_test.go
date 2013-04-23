package gms_test

import (
	"database/sql"
	"fmt"
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

	fmt.Printf("Before\n")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (value BOOL);")
	fmt.Printf("After CREATE TABLE\n")
	_, err = db.Exec("INSERT INTO test VALUES (?)", false)
	fmt.Printf("After INSERT\n")
	_, err = db.Exec("SELECT * FROM test WHERE value = ?;", false)
	fmt.Printf("After SELECT\n")

	iter, err := db.Query("SELECT * FROM test WHERE value = ?", false)
	for iter.Next() {
		var value bool
		err = iter.Scan(&value)
		if err != nil {
			t.Errorf("unexpected Scan error: %v", err)
			break
		}
	}
	iter.Close()
}
