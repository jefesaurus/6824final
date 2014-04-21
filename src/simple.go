package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

func main() {
	os.Remove("./foo.db")

	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sql := `
	create table foo (key INTEGER NOT NULL PRIMARY KEY, value TEXT);
	delete from foo;
	`
	_, err = db.Exec(sql)
	tx, err := db.Begin()
	stmt, err := tx.Prepare("insert into foo(key, value) values(?, ?)")
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		stmt.Exec(i, fmt.Sprintf("こんにちわ世界%03d", i))
	}
	tx.Commit()

	rows, err := db.Query("select key, value from foo")
	defer rows.Close()
	for rows.Next() {
		var key int
		var value string
		rows.Scan(&key, &value)
		fmt.Println(key, value)
	}
	rows.Close()

  fmt.Println("Done with first insert")

  stmt, err = db.Prepare("delete from foo WHERE key = ?")
  defer stmt.Close()
  for i := 0; i < 10; i ++ {
    stmt.Exec(i)
  }
	db.Exec("insert into foo(key, value) values(1, 'foo'), (2, 'bar'), (3, 'baz')")

	rows, err = db.Query("select key, value from foo")
	defer rows.Close()
	for rows.Next() {
		var key int
		var value string
		rows.Scan(&key, &value)
		fmt.Println(key, value)
	}
}
