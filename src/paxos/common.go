package paxos

import "sync"

type Instance struct {
  prepareNum int64 // Highest prepare
  acceptNum int64 // Highest accept number
  acceptVal interface{} // Highest accept value
  decided bool
  //mu sync.Mutex
  multi sync.Mutex
}

type GetMaxArgs struct {
}

type GetMaxReply struct {
  Max int
}
type GetValueArgs struct {
  Seq int
}

type GetValueReply struct {
  OK bool
  Val interface{}
}

type ForwardedArgs struct {
  Seq int
  Val interface{}
}

type ForwardedReply struct {
}

type PingArgs struct {
  Me int
  Done int
}

type PingReply struct {
  OK bool
  Done int
}

type LeaderArgs struct {
}

type LeaderReply struct {
  Leader int
  Ok bool
}

type AcceptArgs struct {
  Me int
  Seq int
  Ok bool
  Num int64
  Done int
  Val interface{}
}
type PrepareArgs struct {
  Seq int
  Num int64
  Me int
  Done int
}
type DecidedArgs struct {
  Seq int
  Num int64
  Val interface{}
}

type AcceptReply struct {
  Ok bool
  Decided bool
  Num int64
  Done int
  Val interface{}
}

type PrepareReply struct {
  Ok bool
  Num int64
  Decided bool
  Done int
  Val interface{}
}

type DecidedReply struct {
  Ok bool
}

func max(a int, b int) int {
  if a > b {
    return a
  }
  return b
}

func min(a int, b int) int {
  if a < b {
    return a
  }
  return b
}
