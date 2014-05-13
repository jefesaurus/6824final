package dbaccess

import (
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
  //"github.com/jmhodges/levigo"
	"fmt"
  "log"
  "encoding/binary"
  //"bytes"
  "strconv"
  "strings"
  "encoding/gob"
  "bytes"
  "sync"
)

// Sync: false corrupts the DB
var opts = &db.WriteOptions{Sync: false}

type Table byte


type DBClerk struct {
  filepath string
  mu sync.Mutex
  db *leveldb.DB
  batch *leveldb.Batch
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
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if ck.isbatching {
    ck.batch.Set(table_key, []byte(value))
  } else {
    err := ck.db.Set(table_key, []byte(value), opts)
	  if err != nil {
		  println(fmt.Sprintf("Failed put(%s, %s): %v", key, value, err))
	  }
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
func (ck *DBClerk) QuietGetString(table Table, key string) (string, bool) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  bytes, err := ck.db.Get(table_key, nil)
  return string(bytes), (err == nil)
}

func (ck *DBClerk) PutInt(table Table, key string, value int) {
  buf := make([]byte, 8)
  _ = binary.PutVarint(buf, int64(value))
  table_key := append([]byte{byte(table)}, []byte(key)...)
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if ck.isbatching {
    ck.batch.Set(table_key, buf)
  } else {
    err := ck.db.Set(table_key, buf, opts)
	  if err != nil {
		  println(fmt.Sprintf("Failed put(%s, %s): %v", key, value, err))
	  }
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

func (ck *DBClerk) QuietGetInt(table Table, key string) (int, bool) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  bytes, err := ck.db.Get(table_key, nil)

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

func (ck *DBClerk) StartBatch() {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if ck.isbatching {
    log.Fatal("Attempting to start batch while already batching")
  }
  ck.isbatching = true
  ck.batch = new(leveldb.Batch)
}

func (ck *DBClerk) EndBatch() {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if !ck.isbatching {
    log.Fatal("Attempting to end batch while not batching")
  }
  ck.isbatching = false
  err := ck.db.Apply(*ck.batch, opts)
  if err != nil {
    log.Fatal(err)
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
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if ck.isbatching {
    ck.batch.Set(table_key, buf.Bytes())
  } else {
    err = ck.db.Set(table_key, buf.Bytes(), opts)
  }
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

func (ck *DBClerk) Delete(table Table, key string) {
  table_key := append([]byte{byte(table)}, []byte(key)...)
  ck.mu.Lock()
  defer ck.mu.Unlock()
  if ck.isbatching {
    ck.batch.Delete(table_key)
  } else {
    err := ck.db.Delete(table_key, opts)
    if err != nil {
      log.Fatal(err)
    }
  }
}

func p(r []byte, e error) {
  if e != nil {
    println("Error: ", e)
  }
  println(string(r))
}
