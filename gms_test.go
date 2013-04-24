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
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (value BOOL, value2 VARCHAR(20) NOT NULL);")
	fmt.Printf("After CREATE TABLE, err=%v\n", err)
	_, err = db.Exec("INSERT INTO test VALUES (?, ?)", nil, "from nil land")
	fmt.Printf("After INSERT, err=%v\n", err)
	_, err = db.Exec("SELECT * FROM test WHERE value = ?;", false)
	fmt.Printf("After SELECT, err=%v\n", err)

	iter, err := db.Query("SELECT value, value2 FROM test WHERE 1 = 1")
	for iter.Next() {
		var value *bool
		var value2 *string
		err = iter.Scan(&value, &value2)
		if err != nil {
			t.Errorf("unexpected Scan error: %v", err)
			break
		}
	}
	iter.Close()
}
