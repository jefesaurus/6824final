package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  sm *shardmaster.Clerk
  config shardmaster.Config
  // You'll have to modify Clerk.
  me int
  seq int
}



func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  // You'll have to modify MakeClerk.
  ck.me = nrandint()
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &GetArgs{Key: key, Me: ck.me, Seq: ck.seq,  ConfigNum: ck.config.Num}
        var reply GetReply
        if ok := call(srv, "ShardKV.Get", args, &reply); ok {
          switch reply.Err {
          case OK, ErrNoKey:
            ck.seq ++
            return reply.Value
          case ErrWrongGroup:
            break
          }
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
  return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &PutArgs{Key: key, Value: value, DoHash: dohash, Me: ck.me, Seq: ck.seq, ConfigNum: ck.config.Num}
        var reply PutReply
        if ok := call(srv, "ShardKV.Put", args, &reply); ok {
          switch reply.Err {
          case OK:
            ck.seq ++
            return reply.PreviousValue
          case ErrWrongGroup:
            break
          }
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
