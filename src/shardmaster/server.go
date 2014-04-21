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

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  pxMu sync.Mutex

  configs []Config // indexed by config num
  queryReplies map[int]Config // Keep track of replies
  lastApplied int
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
  config := sm.queryReplies[seq]
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
      sm.lastApplied = seq
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

  if !sm.doneWith[seq] { // We apply it, and calculate the reply if we haven't already seen it
    if op.Opcode == QUERY {
      if op.Num < 0 || op.Num > len(sm.configs) {
        sm.queryReplies[seq] = CopyConfig(&sm.configs[len(sm.configs)-1])
      } else {
        sm.queryReplies[seq] = CopyConfig(&sm.configs[op.Num])
      }
    } else {
      conf := CopyConfig(&sm.configs[len(sm.configs) - 1])
      conf.Num ++
      switch op.Opcode {
      case JOIN: conf.ApplyJoin(op.GID, op.Servers)
      case LEAVE: conf.ApplyLeave(op.GID)
      case MOVE: conf.ApplyMove(op.Num, op.GID)
      }
      sm.configs = append(sm.configs, conf)
    }
    sm.doneWith[seq] = true
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
  previousGID := conf.Shards[Shard]
  conf.Shards[Shard] = GID
  isGone := true
  for _, gid := range conf.Shards {
    if gid == previousGID {
      isGone = false
      break
    }
  }
  if isGone {
    //delete(conf.Groups, previousGID)
  }
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

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.queryReplies = make(map[int]Config)
  sm.doneWith = make(map[int]bool)
  sm.lastApplied = -1

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
