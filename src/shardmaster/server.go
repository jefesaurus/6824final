package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math"
import "math/rand"
import "time"
import "dbaccess"
import "strconv"
import "strings"


type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  db *dbaccess.DBClerk
  db_path string

  px *paxos.Paxos
  pxMu sync.Mutex

  configs []Config // indexed by config num
  queryReplies map[int]Config // Keep track of replies
  doneWith map[int]bool // Ops that are totally done
}

const (
  JOIN = 0
  LEAVE = 1
  MOVE = 2
  QUERY = 3
)

type Op struct {
  Opcode int
  Num int
  GID int64
  Servers []string
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  thisOp := Op{Opcode: JOIN, GID: args.GID, Servers: args.Servers}
  sm.StartAndFinishOp(thisOp)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  thisOp := Op{Opcode: LEAVE, GID: args.GID}
  sm.StartAndFinishOp(thisOp)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  thisOp := Op{Opcode: MOVE, GID: args.GID, Num: args.Shard}
  sm.StartAndFinishOp(thisOp)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  thisOp := Op{Opcode: QUERY, Num: args.Num}
  seq := sm.StartAndFinishOp(thisOp)
  //config := sm.queryReplies[seq]
  config := sm.GetReply(seq)
  reply.Config = config
  return nil
}

func (sm *ShardMaster) StartAndFinishOp(op Op) int {
  seq := sm.ForceSubmitOp(op)
  for !sm.doneWith[seq] {
    time.Sleep(2 * time.Millisecond)
  }
  sm.px.Done(seq - 1)
  return seq
}

func (sm *ShardMaster) ForceSubmitOp(op Op) int {
  submitted := false
  var seq int
  for !submitted && !sm.dead {
    seq = sm.px.Max() + 1
    submitted = sm.SubmitOp(op, seq)
  }
  return seq
}

func (sm *ShardMaster) SubmitOp(op Op, seq int) bool {
  if sm.doneWith[seq] {
    decided, acceptVal := sm.px.Status(seq)
    return decided && OpEquals(acceptVal.(Op), op)
  }
  sm.px.Start(seq, op)

  for !sm.dead {
    decided, acceptVal := sm.px.Status(seq)
    if decided {
      if OpEquals(acceptVal.(Op), op) {
        return true
      } else {
        return false
      }
    }
    time.Sleep(10 * time.Millisecond)
  }
  return false
}

/*
Walks up the log, applies decided stuff, and forces decisions on undecided stuff
*/
func (sm *ShardMaster) LogWalker() {
  seq := sm.px.Min()
  timeout := 10
  for !sm.dead {
    if decided, opData := sm.px.Status(seq); decided {
      timeout = 10
      sm.ApplyOp(seq, opData.(Op))
      seq ++
    } else {
      sm.px.Start(seq, Op{Opcode: QUERY, Num:-1})
      time.Sleep(time.Duration(timeout) * time.Millisecond)
      if timeout < 250 {
        timeout *= 2
      }
    }
  }
}

func (sm *ShardMaster) ApplyOp(seq int, op Op) bool {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  config_num := sm.DBGetNumConfigs()

  if !sm.GetDone(seq) { // We apply it, and calculate the reply if we haven't already seen it
    if op.Opcode == QUERY {
      var rep Config
      if op.Num < 0 || op.Num > config_num {
        rep = sm.GetConfig(config_num-1)
      } else {
        rep = sm.GetConfig(op.Num)
      }
      sm.PutReply(seq, &rep)
    } else {
      conf := sm.GetConfig(config_num - 1)
      conf.Num ++
      switch op.Opcode {
      case JOIN: conf.ApplyJoin(op.GID, op.Servers)
      case LEAVE: conf.ApplyLeave(op.GID)
      case MOVE: conf.ApplyMove(op.Num, op.GID)
      }
      sm.PutConfig(config_num, &conf)
    }
    //sm.doneWith[seq] = true
    sm.SetDone(seq)
    return true
  }
  return false
}

func OpEquals(op1 Op, op2 Op) bool {
  if op1.Opcode == op2.Opcode &&
     op1.Num == op2.Num &&
     op1.GID == op2.GID {
     if len(op1.Servers) == len(op2.Servers) { //not quite perfect
      return true
     }
  }
  return false
}

func (conf *Config) ApplyJoin(GID int64, Servers []string) {
  conf.Groups[GID] = Servers
  gids := make(map[int64][]int)
  gids[GID] = []int{}
  for gid, _ := range conf.Groups {
    gids[gid] = []int{}
  }
  for shard, gid := range conf.Shards {
    if gid == 0 {
      gids[GID] = append(gids[GID], shard)
    } else {
      gids[gid] = append(gids[gid], shard)
    }
  }
  conf.Balance(gids)
}

func (conf *Config) ApplyLeave(GID int64) {
  delete(conf.Groups, GID)
  if len(conf.Groups) > 0 {
    gids := make(map[int64][]int)
    for gid, _ := range conf.Groups {
      gids[gid] = []int{}
    }
    target := int64(-1)
    for shard, gid := range conf.Shards {
      gids[gid] = append(gids[gid], shard)
      if target < 0 && gid != GID {
        target = gid
      }
    }
    gids[target] = append(gids[target], gids[GID]...)
    delete(gids, GID)
    conf.Balance(gids)
  } else {
    for i := 0; i < len(conf.Shards); i ++ {
      conf.Shards[i] = 0
    }
  }
}

func (conf *Config) ApplyMove(Shard int, GID int64) {
  conf.Shards[Shard] = GID
}

func CopyConfig(old *Config) Config {
  newConfig := Config{Num: old.Num, Shards: old.Shards, Groups: make(map[int64][]string)}
  for gid, servers := range old.Groups {
    newConfig.Groups[gid] = servers
  }
  return newConfig
}

func (conf *Config) Balance(gids map[int64][]int) {
  s, b, ss, bs := ArgRange(gids)
  for bs - ss > 1 {
    gids[s] = append(gids[s], gids[b][len(gids[b])-1])
    gids[b] = gids[b][:len(gids[b])-1]
    s, b, ss, bs = ArgRange(gids)
  }
  for gid, shards := range gids {
    for _, shard := range shards {
      conf.Shards[shard] = gid
    }
  }
}

// Returns min, max and respective keys
func ArgRange(gids map[int64][]int) (int64,int64,int,int) {
  maxVal := math.MinInt32
  maxKey := int64(math.MinInt64)
  minVal := math.MaxInt32
  minKey := int64(math.MaxInt64)
  for key, slice := range gids {
    n := len(slice)
    if n > maxVal || n == maxVal && key > maxKey {
      maxVal = n
      maxKey = key
    }
    if n < minVal || n == minVal && key < minKey{
      minVal = n
      minKey = key
    }
  }
  return minKey, maxKey, minVal, maxVal
}

func (sm *ShardMaster) GetConfig(config_num int) Config {
  //mem_conf := sm.configs[config_num]
  db_conf := sm.DBGetConfig(config_num)
  //CheckEq(&mem_conf, db_conf)
  return *db_conf
}

func (sm *ShardMaster) PutConfig(config_num int, conf *Config) {
  //sm.configs = append(sm.configs, *conf)
  sm.DBPutConfig(config_num, conf)
  sm.DBIncNumConfigs()
}

func CheckEq(a *Config, b *Config){
  if a.Num != b.Num {
    fmt.Printf("Not the same: %d, %d\n", a.Num, b.Num)
  }
  for i, anum := range a.Shards {
    if b.Shards[i] != anum {
      fmt.Println("Not the same")
    }
  }
}

func (sm *ShardMaster) PutReply(seq int, conf *Config) {
  //sm.queryReplies[seq] = *conf
  sm.DBPutReply(seq, conf)
}

func (sm *ShardMaster) GetReply(seq int) Config {
  //mem_conf := sm.queryReplies[seq]
  db_conf := sm.DBGetReply(seq)
  //CheckEq(&mem_conf, db_conf)
  return *db_conf
}

func (sm *ShardMaster) SetDone(seq int) {
  sm.doneWith[seq] = true
  sm.DBSetDone(seq)
}

func (sm *ShardMaster) GetDone(seq int) bool {
  mem_done := sm.doneWith[seq]
  db_done := sm.DBGetDone(seq)
  if mem_done != db_done {
    fmt.Println("NOT EQUAL")
  }
  return db_done
}


//######################################################################
const (
  METADATA = 10
  CONFIG_TABLE = 11
  REPLY_TABLE = 12
  DONE_TABLE = 13
)

/*
  configs []Config // indexed by config num
  queryReplies map[int]Config // Keep track of replies
  doneWith map[int]bool // Ops that are totally done
*/

func (sm *ShardMaster) DBGetNumConfigs() int {
  num_configs, exists := sm.db.GetInt(METADATA, "num_configs")
  if !exists {
    log.Fatal("No config count in db")
  }
  return num_configs
}
func (sm *ShardMaster) DBIncNumConfigs() {
  num_configs := sm.DBGetNumConfigs()
  sm.db.PutInt(METADATA, "num_configs", num_configs+1)
}

func (sm *ShardMaster) DBPutConfig(config_num int, conf *Config) {
  sm.db.PutStruct(CONFIG_TABLE, strconv.Itoa(config_num), conf)
}

func (sm *ShardMaster) DBGetConfig(config_num int) *Config {
  conf := &Config{}
  sm.db.GetStruct(CONFIG_TABLE, strconv.Itoa(config_num), conf)
  return conf
}

func (sm *ShardMaster) DBPutReply(seq int, conf *Config) {
  sm.db.PutStruct(REPLY_TABLE, strconv.Itoa(seq), conf)
}

func (sm *ShardMaster) DBGetReply(seq int) *Config {
  conf := &Config{}
  sm.db.GetStruct(REPLY_TABLE, strconv.Itoa(seq), conf)
  return conf
}

func (sm *ShardMaster) DBSetDone(seq int) {
  sm.db.PutInt(DONE_TABLE, strconv.Itoa(seq), 1)
}

func (sm *ShardMaster) DBGetDone(seq int) bool {
  done_num, exists := sm.db.QuietGetInt(DONE_TABLE, strconv.Itoa(seq))
  if !exists {
    return false
  }
  return (done_num > 0)
}


//######################################################################

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

const db_path = "/tmp/smdb/"

func (sm *ShardMaster) FreshStart(servers []string, me int) {
  // Put my index
  sm.db.PutInt(METADATA, "me", me)
  sm.me = me

  // Enter peers
  sm.db.PutStringList(METADATA, "servers", servers)

  first_conf := new(Config)
  first_conf.Groups = map[int64][]string{}
  sm.DBPutConfig(0, first_conf)
  sm.db.PutInt(METADATA, "num_configs", 1)
}

func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  sm := new(ShardMaster)

  path_parts := strings.Split(servers[me], "/")
  sm.db_path = db_path + path_parts[len(path_parts)-1]
  os.Remove(sm.db_path)
  sm.db = dbaccess.GetDatabase(sm.db_path)

  sm.FreshStart(servers, me)
  FinishStartServer(sm, servers, me)
  return sm
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func FinishStartServer(sm *ShardMaster, servers []string, me int) *ShardMaster {

  //sm.configs = make([]Config, 1)
  //sm.configs[0].Groups = map[int64][]string{}

  //sm.queryReplies = make(map[int]Config)

  sm.doneWith = make(map[int]bool)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  go sm.LogWalker()

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
