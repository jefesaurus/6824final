package shardkv
import "hash/fnv"
import "shardmaster"
import "math/big"
import "crypto/rand"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrNoConfig = "ErrNoConfig"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool

  // Added
  Me int
  Seq int
  ConfigNum int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string

  // added
  Me int
  Seq int
  ConfigNum int
}

type GetReply struct {
  Err Err
  Value string
}

type SendShardArgs struct {
  ShardNum int
  ConfigNum int
}

type SendShardReply struct {
  Shard map[string]string
  Replies map[int]ReplyRecord

  Err Err
}

type ReconfigArgs struct {
  Config int
}

type ReconfigReply struct {
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}

func nrandint() int {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return int(x)
}
