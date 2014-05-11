package paxos

import (
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"fmt"
  "sync"
  "log"
  "encoding/binary"
  //"bytes"
  "strconv"
  "strings"
  "encoding/gob"
  "bytes"
)

// Sync: false corrupts the DB
var opts = &db.WriteOptions{Sync: true}

const (
  METADATA = 1
  STORAGE = 2
)

type Table byte

type DBClerk struct {
  filepath string
  mu sync.Mutex
  db *leveldb.DB
  isbatching bool
}

func GetDatabase(filepath string) *DBClerk {
  ck := new(DBClerk)
  ck.filepath = filepath

	db, err := leveldb.Open(filepath, nil)
	if err != nil {
		log.Fatal("Could not open LevelDB at %s: %v\n", filepath, err)
	}
  ck.db = db
  return ck
}
func (ck *DBClerk) Close() {
  ck.db.Close()
}

func (ck *DBClerk) PutString(table Table, key string, value string) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  err := ck.db.Set(table_key, []byte(value), opts)
	if err != nil {
		println(fmt.Sprintf("Failed put(%s, %s): %v", key, value, err))
	}
}

func (ck *DBClerk) BatchPutString(table Table, key string, value string, batch *leveldb.Batch) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  batch.Set(table_key, []byte(value))
}

func (ck *DBClerk) GetString(table Table, key string) (string, bool) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  bytes, err := ck.db.Get(table_key, nil)
	if err != nil {
		println(fmt.Sprintf("Failed get(%s): %v", key, err))
	}
  return string(bytes), (err == nil)
}

func (ck *DBClerk) PutInt(table Table, key string, value int) {
  buf := make([]byte, 8)
  _ = binary.PutVarint(buf, int64(value))

  table_key := append([]byte{byte(table)}, []byte(key)...)
  err := ck.db.Set(table_key, buf, opts)
	if err != nil {
		println(fmt.Sprintf("Failed put(%s, %s): %v", key, value, err))
	}
}

func (ck *DBClerk) GetInt(table Table, key string) (int, bool) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  bytes, err := ck.db.Get(table_key, nil)

	if err != nil {
		println(fmt.Sprintf("Failed get(%s): %v", key, err))
	}

  val, _ := binary.Varint(bytes)
  return int(val), (err == nil)
}

func (ck *DBClerk) KeyExists(table Table, key string) bool {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  _, err := ck.db.Get(table_key, nil)
	return (err == nil)
}

func (ck *DBClerk) PutIntList(table Table, key string, values []int) {
  string_rep := strconv.Itoa(values[0])
  for _, value := range values[1:len(values)] {
    string_rep += fmt.Sprintf(",%s",strconv.Itoa(value))
  }
  ck.PutString(table, key, string_rep)
}

func (ck *DBClerk) GetIntList(table Table, key string) ([]int, bool) {
  string_rep, success := ck.GetString(table, key)

  if success {
    str_ints := strings.Split(string_rep, ",")
    final_ints := make([]int, len(str_ints))
    for i, str_int := range str_ints {
      real_int, _ := strconv.Atoi(str_int)
      final_ints[i] = real_int
    }
    return final_ints, true
  } else {
    return nil, false
  }
}

const STRING_SEP = ","
func (ck *DBClerk) PutStringList(table Table, key string, values []string) {
  string_rep := strings.Join(values, STRING_SEP)
  ck.PutString(table, key, string_rep)
}

func (ck *DBClerk) GetStringList(table Table, key string) ([]string, bool) {
  string_rep, success := ck.GetString(table, key)

  if success {
    strs := strings.Split(string_rep, STRING_SEP)
    return strs, true
  } else {
    return nil, false
  }
}


func (ck *DBClerk) ApplyBatch(b leveldb.Batch) {
  err := ck.db.Apply(b, opts)
  if err != nil {
    log.Fatal(err)
  }
}

func (ck *DBClerk) PutStruct(table Table, key string, o interface{}) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
  err := enc.Encode(o)
  if err != nil {
    log.Fatal(err)
  }
  err = ck.db.Set(table_key, buf.Bytes(), opts)
  if err != nil {
    log.Fatal(err)
  }
}

func (ck *DBClerk) GetStruct(table Table, key string, o interface{}) bool {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  var buf bytes.Buffer

  b, err := ck.db.Get(table_key, nil)
  key_exists := (err == nil)
  _, err = buf.Write(b)
  if err != nil {
    log.Fatal(err)
  }
  dec := gob.NewDecoder(&buf)
  dec.Decode(o)
  return key_exists
}

func test() {
  db_path := "/tmp/testdb"
  ck := GetDatabase(db_path)

	key := "testkey"
  data := "testvalue1"
  data2 := "testvaluea"

  ck.PutString(METADATA, key, data)
  ck.PutString(STORAGE, key, data2)
  ck.Close()

  ck = GetDatabase(db_path)
  val, _ := ck.GetString(METADATA, key)

  if val != data {
    println("Results do not match")
  }
  println(val)


  val, _ = ck.GetString(STORAGE, key)
  if val != data2 {
    println("Results do not match")
  }
  println(val)


  val, _ = ck.GetString(METADATA, key)

  if val != data {
    println("Results do not match")
  }
  println(val)

  ck.Close()
  ck = GetDatabase(db_path)
  data = "testvalue2"
  ck.PutString(METADATA, key, data)
  ck.Close()
  ck = GetDatabase(db_path)
  val, _ = ck.GetString(METADATA, key)

  if val != data {
    println("Results do not match")
  }
  println(val)

	fmt.Printf("DONE.\n")
}

func testintlist() {
  db_path := "/tmp/testdb"
  ck := GetDatabase(db_path)

	key := "testkey"
  data := []int{4,3,2,1}

  ck.PutIntList(METADATA, key, data)
  ck.Close()

  ck = GetDatabase(db_path)
  val, _ := ck.GetIntList(METADATA, key)

  for i := 0; i < len(data); i ++ {
    if (val[i] != data[i]) {
      println("Int lists do not match")
    }
  }
  ck.Close()
}

func test_speed(iters int) {
  db_path := "/tmp/testdb"
  ck := GetDatabase(db_path)

  for i := 0; i < iters; i ++ {
    ck.PutInt(METADATA, fmt.Sprintf("k: %d", i), i)
    println(fmt.Sprintf("val-written: %d", i))
  }

  for i := 0; i < iters; i ++ {
    v, _ := ck.GetInt(METADATA, fmt.Sprintf("k: %d", i))
    println(fmt.Sprintf("val-read: %d", v))
  }
  ck.Close()
}

func test_batch() {
  db_path := "/tmp/testdbb"
  ck := GetDatabase(db_path)
  var b leveldb.Batch
  ck.BatchPutString(METADATA, "key1", "val1", &b)
  ck.BatchPutString(METADATA, "key2", "val2", &b)
  ck.ApplyBatch(b)

  val, exists := ck.GetString(METADATA, "key1")
  if !exists {
    println("key1 didn't exist1")
  }
  println(val)

  val, exists = ck.GetString(METADATA, "key2")
  if !exists {
    println("key2 didnt' exist2")
  }
  println(val)
}

type Instanz struct {
  PrepareNum int64 // Highest prepare
  AcceptNum int64 // Highest accept number
  AcceptVal interface{} // Highest accept value
  Decided bool
}

func test_struct() {
  inst := &Instanz{55, 66, "asdf", true}

  db_path := "/tmp/testdbb1"
  ck := GetDatabase(db_path)
  ck.PutStruct(METADATA, "first", inst)
  ck.Close()
  ck = GetDatabase(db_path)
  inst2 := new(Instanz)
  ck.GetStruct(METADATA, "first", inst2)
  println(inst2.AcceptVal.(string))
  println(inst2.AcceptNum)
  println(inst2.PrepareNum)
}



func main() {
  //test_batch()
  //test_speed(500)
  test_struct()
}


func p(r []byte, e error) {
  if e != nil {
    println("Error: ", e)
  }
  println(string(r))
}


