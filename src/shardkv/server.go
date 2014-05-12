package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

// OPCODES
const (
  NOP = 0
  GET = 1
  PUT = 2
  RECONFIG = 4 // Only interesting op. Drops a marker in the paxos log
)

type Op struct {
  // Your definitions here.
  ID OpID
  Opcode int

  // For get and put
  Key string

  // For put
  Value string
  DoHash bool

  // For reconfig point agreement
  ConfigNum int
}

// Should be unique to the OP
type OpID struct {
  Me int
  Seq int
}

type ReplyRecord struct {
  ID OpID
  Reply string
  Err Err
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  config shardmaster.Config

  // shards num mapping to key/value mapping
  store map[int]map[string]string
  reply_signal map[OpID]chan bool
  last_reply map[int]ReplyRecord

  // Records for download
  shard_record map[int]map[int]map[string]string
  reply_record map[int]map[int]ReplyRecord

  // As a client
  seq int

  record_level int // Level of usable records for download
  config_mu sync.Mutex // Locks the main config

  min_confignums map[int64]int
  config_to_seq map[int]int
  confignums_lock sync.Mutex
}

func (kv *ShardKV) ManageConfigMins() {
  for !kv.dead {
    //determine our local min
    min_seq := kv.px.Min() - 1
    kv.confignums_lock.Lock() 
    max_config := kv.min_confignums[kv.gid]
    for key, val := range kv.config_to_seq {
      if min_seq >= val && key > max_config {
        max_config = key
        delete(kv.config_to_seq, key)
      } 
    }
    kv.min_confignums[kv.gid] = max_config 
    kv.confignums_lock.Unlock() 

    //Forget old config nums
    mini := kv.MinGlobalConfigNum()
    for key, _ := range kv.shard_record {
      if key <= mini {
        delete(kv.shard_record, key)
        delete(kv.reply_record, key)
      }      
    }
 
    //Send out our min to peers
    conf := kv.config
    for gid, peers := range conf.Groups {
      for _, server := range peers {
        reply := &MinConfigReply{}
        rpc := make(chan bool, 1)
        go func() { rpc <- call(server, "ShardKV.MinConfig", &MinConfigArgs{kv.gid, max_config}, reply) }()
        select {
          case ok := <- rpc:
            if ok {
              kv.confignums_lock.Lock()
              kv.min_confignums[gid] = min(kv.min_confignums[gid], reply.Min)
              kv.confignums_lock.Unlock()
              break
            }
          case <-time.After(time.Second):
        }
      }
    }
    time.Sleep(10 * time.Second)
  }
}

func (kv *ShardKV) MinConfig(args *MinConfigArgs, reply *MinConfigReply) error {
  kv.confignums_lock.Lock()
  kv.min_confignums[args.Me] = min(kv.min_confignums[args.Me], args.Min)
  kv.confignums_lock.Unlock()
  reply.Min = kv.min_confignums[kv.gid]  
  return nil
}

func (kv *ShardKV) MinGlobalConfigNum() int {
  min := (int) (^uint(0) >> 1)
  kv.confignums_lock.Lock()
  defer kv.confignums_lock.Unlock()
  for _, val := range kv.min_confignums {
    if val < min {
      min = val
    }
  }
  return min
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  op_id := OpID{args.Me, args.Seq}
  this_op := Op{ID: op_id, Opcode: GET, Key: args.Key}
  reply_rec := kv.SubmitAndGetReply(this_op)
  reply.Err = reply_rec.Err
  reply.Value = reply_rec.Reply
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  if (args.ConfigNum > kv.config.Num) { // Helps keep the client config close to server configs
    reply.Err = ErrWrongGroup
    return nil
  }
  op_id := OpID{args.Me, args.Seq}
  this_op := Op{ID: op_id, Opcode: PUT, Key: args.Key, Value: args.Value, DoHash: args.DoHash}
  reply_rec := kv.SubmitAndGetReply(this_op)
  reply.Err = reply_rec.Err
  reply.PreviousValue = reply_rec.Reply
  return nil
}

func (kv *ShardKV) SubmitAndGetReply(op Op) ReplyRecord {
  id := op.ID
  kv.reply_signal[id] = make(chan bool, 1)
  kv.ForceSubmitOp(op)
  IsWrongGroup := <-kv.reply_signal[id]
  delete(kv.reply_signal, id)
  var record ReplyRecord
  if IsWrongGroup {
    record = ReplyRecord{ID: op.ID, Err: ErrWrongGroup}
  } else {
    record = kv.last_reply[id.Me]
  }
  return record
}

// Forces a submit of the OP, returns the sequence number it was successfully
// submitted with
func (kv *ShardKV) ForceSubmitOp(op Op) int {
  submitted := false
  var seq int
  for !submitted && !kv.dead {
    seq = kv.px.Max() + 1
    submitted = kv.SubmitOp(seq, op)
  }
  return seq
}

func (kv *ShardKV) SubmitOp(seq int, op Op) bool {
  kv.px.Start(seq, op)
  for !kv.dead {
    decided, accepted_val := kv.px.Status(seq)
    if decided {
      return accepted_val.(Op).ID == op.ID
    }
    time.Sleep(10 * time.Millisecond)
  }
  return false
}

func (kv *ShardKV) SetReconfigPoint(new_config int) {
  if new_config == kv.config.Num + 1 {
    op_id := OpID{kv.me, kv.seq}
    this_op := Op{ID: op_id, Opcode: RECONFIG, ConfigNum: new_config}
    kv.ForceSubmitOp(this_op)
  }
  kv.seq++
  return
}

func (kv *ShardKV) InitNewConfig(new_config shardmaster.Config) {
  old_config := kv.GetConfig()

  // Update the record with current stuff
  kv.shard_record[old_config.Num] = make(map[int]map[string]string)
  kv.reply_record[old_config.Num] = make(map[int]ReplyRecord)
  for client_id, last_record := range kv.last_reply {
    this_rec := ReplyRecord{last_record.ID, last_record.Reply, last_record.Err}
    kv.reply_record[old_config.Num][client_id] = this_rec
  }

  for shard, new_owner := range new_config.Shards {
    old_owner := old_config.Shards[shard]
    if old_owner == kv.gid && new_owner != kv.gid{
      kv.shard_record[old_config.Num][shard] = make(map[string]string)
      for k, v := range kv.store[shard] {
        kv.shard_record[old_config.Num][shard][k] = v
      }
    }
  }
  
  // At this point, the records are good up to this level
  kv.record_level = old_config.Num

  // Download new stuff
  for shard, new_owner := range new_config.Shards {
    old_owner := old_config.Shards[shard]
    if new_owner == kv.gid {
      if old_owner == 0 { // First time this shard is used
        kv.store[shard] = make(map[string]string)
      } else if  old_owner != kv.gid  {
        kv.DownloadShard(shard, old_owner, old_config.Num)
      }
    }
  }
  kv.SetConfig(new_config)
  kv.confignums_lock.Lock()
  defer kv.confignums_lock.Unlock()
  for gid, _ := range new_config.Groups {
    if _, ok := kv.min_confignums[gid]; !ok {
      kv.min_confignums[gid] = new_config.Num
    }
  }
  for gid, _ := range kv.min_confignums {
    if _, ok := new_config.Groups[gid]; !ok {
      delete(kv.min_confignums, gid)
    }
  }
}

// Swoop data from other replica groups
func (kv *ShardKV) DownloadShard(shard_num int, group int64, config_num int) {
  args := SendShardArgs{ShardNum: shard_num, ConfigNum: config_num}
  var reply SendShardReply

  curr_config := kv.GetConfig()
  servers := curr_config.Groups[group]
  current_server := 0
  for {
    if ok := call(servers[current_server], "ShardKV.SendShard", &args, &reply); ok {
      if reply.Err == OK {
        kv.store[shard_num] = reply.Shard // Stash shards
        // Sequential Consistency is staisfied with this next part:
        for client, reply_record := range reply.Replies { // Apply the downloaded ones that supercede
          current_client_record, it_exists := kv.last_reply[client]
          if !it_exists || current_client_record.ID.Seq <= reply_record.ID.Seq {
            kv.last_reply[client] = reply_record
          }
        }
        return
      }
    }
    current_server ++
    current_server %= len(servers)
    time.Sleep(25 * time.Millisecond)
  }
  return
}

// Handler that supplies shard/reply data to requestors
func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) error {
  if args.ConfigNum <= kv.record_level { // Make sure we are far enough along to supply
    reply.Shard = kv.shard_record[args.ConfigNum][args.ShardNum]
    reply.Replies = kv.reply_record[args.ConfigNum]
    reply.Err = OK
  } else {
    reply.Err = ErrNoConfig
  }
  return nil
}

// This is the routine that sequentially handles all paxos agreements
func (kv *ShardKV) LogWalker() {
  seq := kv.px.Min()
  timeout := 10
  for !kv.dead {
    if decided, op_data := kv.px.Status(seq); decided {
      kv.ApplyOp(op_data.(Op), seq)
      kv.px.Done(seq)
      seq++
      timeout = 10
    } else {
      if kv.px.Max() > seq {
        kv.px.Start(seq, Op{Opcode: NOP})
      }
      time.Sleep(time.Duration(timeout) * time.Millisecond)
      if timeout < 250 {
        timeout *= 2
      }
    }
  }
}

func (kv *ShardKV) ApplyOp(op Op, seq int) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  reply_string := "default-reply-string"
  var reply_err Err
  reply_err = OK
  if record, exists := kv.last_reply[op.ID.Me]; !(exists && record.ID == op.ID) { // Filter Dups
    switch op.Opcode {
    case GET, PUT:
      shard := key2shard(op.Key)
      if kv.config.Shards[shard] == kv.gid {
        if op.Opcode == GET {
          stored_val, key_exists := kv.store[shard][op.Key]
          if key_exists {
            reply_string = stored_val
          } else {
            reply_err = ErrNoKey
            reply_string = "no key!"
          }
        } else {
          if op.DoHash {
            reply_string = kv.store[shard][op.Key]
            kv.store[shard][op.Key] = strconv.Itoa(int(hash(reply_string + op.Value)))
          } else {
            reply_string = ""
            kv.store[shard][op.Key] = op.Value
          }
        }
      } else {
        reply_err = ErrWrongGroup
      }
    case RECONFIG:
      if op.ConfigNum == kv.config.Num + 1 {
        new_config := kv.sm.Query(op.ConfigNum) // Get next configuration
        kv.InitNewConfig(new_config)            // Initialize it
        kv.config_to_seq[op.ConfigNum] = seq 
      }
    }
    if reply_err != ErrWrongGroup {
      kv.last_reply[op.ID.Me] = ReplyRecord{ID:op.ID, Reply:reply_string, Err: reply_err} // record this op
    }
  }

  if signal_channel, channel_exists := kv.reply_signal[op.ID]; channel_exists {
    if reply_err == ErrWrongGroup { // Super ghetto way to signal the ErrWrongGroup without using th reply log
      signal_channel <- true
    } else {
      signal_channel <- false
    }
  }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  master_conf := kv.sm.Query(-1)
  if master_conf.Num > kv.config.Num {
    kv.SetReconfigPoint(kv.config.Num + 1)
  }
}

//Idfk if these are necessary but whatevs
func (kv *ShardKV) GetConfig() shardmaster.Config {
  kv.config_mu.Lock()
  copped := shardmaster.CopyConfig(&kv.config)
  kv.config_mu.Unlock()
  return copped
}

func (kv *ShardKV) SetConfig(new_config shardmaster.Config) {
  kv.config_mu.Lock()
  kv.config = new_config
  kv.config_mu.Unlock()
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.reply_signal = make(map[OpID]chan bool)
  kv.store = make(map[int]map[string]string)
  kv.last_reply = make(map[int]ReplyRecord)

  kv.shard_record = make(map[int]map[int]map[string]string)
  kv.reply_record = make(map[int]map[int]ReplyRecord)
  kv.record_level = -1
  kv.min_confignums = make(map[int64]int)

  kv.InitNewConfig(kv.sm.Query(-1))

  rpcs := rpc.NewServer()
  rpcs.Register(kv)
  kv.px = paxos.Make(servers, me, rpcs)

  kv.config_to_seq = make(map[int]int)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  go kv.LogWalker()
  go kv.ManageConfigMins()
  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    time.Sleep(time.Duration(kv.me * 10) * time.Millisecond)
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
