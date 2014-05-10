package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
  //"strconv"
  "sync"
  //"reflect"
)

type DBClerk struct {
  filepath string
  mu sync.Mutex
  db *sql.DB
}

func GetDatabase(filepath string) *DBClerk {
  ck := new(DBClerk)
  ck.filepath = filepath
  db, err := sql.Open("sqlite3", filepath)
  if err != nil {
    log.Fatal(err)
  }
  ck.db = db
  return ck
}

func (ck *DBClerk) ResetDatabase() {
  ck.Close()  // close connection
	os.Remove(ck.filepath)  // clear db

  db, err := sql.Open("sqlite3", ck.filepath)
  if err != nil {
    log.Fatal(err)
  }
  ck.db = db
}

func (ck *DBClerk) MakeKeyValueTable(table_name string) {
	sql := fmt.Sprintf(`
	create table %s (key TEXT NOT NULL PRIMARY KEY, value TEXT);
	delete from %s;
	`, table_name, table_name)
  _, err := ck.db.Exec(sql)
  if err != nil {
    log.Fatal(err)
  }
}

func (ck *DBClerk) DropKeyValueTable(table_name string) {
	sql := fmt.Sprintf(`
	drop table %s;
	`, table_name, table_name)
  _, err := ck.db.Exec(sql)
  if err != nil {
    log.Fatal(err)
  }
}

func (ck *DBClerk) ListTables() {
  rows, err := ck.db.Query("SELECT name from sqlite_master WHERE type='table'")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    var name string
    rows.Scan(&name)
    fmt.Println(name)
  }
}

func (ck *DBClerk) Close() {
  ck.db.Close()
}

func BurnTest(iterations int) {
  db := GetDatabase(":memory:")
  db.ResetDatabase()
  defer db.Close()
  db.MakeKeyValueTable("store")
  for i := 0; i < iterations; i ++ {
    _, err := db.db.Exec(fmt.Sprintf("insert into store(key, value) values('whoah %d', 'dude')", i))
    fmt.Printf("insert into store(key, value) values('whoah %d', 'dude')\n", i)
    fmt.Println("success")
    if err != nil {
      log.Fatal(err)
    }
  }
}

func BurnTestWithPrepare(iterations int) {
  db := GetDatabase(":memory:")
  db.ResetDatabase()
  defer db.Close()
  db.MakeKeyValueTable("store")
  for i := 0; i < iterations; i ++ {
    _, err := db.db.Exec(fmt.Sprintf("insert into store(key, value) values('whoah %d', 'dude')", i))
    fmt.Printf("insert into store(key, value) values('whoah %d', 'dude')\n", i)
    fmt.Println("success")
    if err != nil {
      log.Fatal(err)
    }
  }

  tx, err := db.db.Begin()
  if err != nil {
    log.Fatal(err)
  }
  stmt, err := tx.Prepare("insert into store(key, value) values(?, ?)")
  if err != nil {
    log.Fatal(err)
  }
  defer stmt.Close()
  for i := 0; i < 100; i++ {
    _, err = stmt.Exec(fmt.Sprintf("Whoah %d", i), fmt.Sprintf("Dude %d", i))
    if err != nil {
      log.Fatal(err)
    }
  }
  tx.Commit()
}

func MakeDBTest() {
  db := GetDatabase("fool.db")
  defer db.Close()
  db.ListTables()
  db.DropKeyValueTable("wtt")
  //db.MakeKeyValueTable("wtt")
}

func main() {
  BurnTest(1000)
}
