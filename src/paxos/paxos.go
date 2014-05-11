package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type Paxos struct {
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  majority int

  instances map[int]*Instance
  //instancesLock sync.Mutex
  instanceDataLock sync.RWMutex   

  max int
  maxLock sync.Mutex

  mins []int
  minsLock sync.Mutex

  prevDone int

  PingTimes map[int]time.Time
  Leader bool
  PingLock sync.Mutex
  LeaderLock sync.RWMutex
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

const (
  PING_INTERVAL = time.Second * 5
  LEADER_LEASE = PING_INTERVAL * 2
  PING_TIMEOUT = time.Millisecond * 100
  PAXOS_TIMEOUT = time.Second * 3
)

func (px *Paxos) MultiPaxos() {
  go func() {
  for true {
    if px.dead {
      return
    }

    for index, server := range px.peers {
      if index == px.me {
        continue
      }
      reply := &PingReply{}
      rpc := make(chan bool, 1)
      go func() { rpc <- call(server, "Paxos.Ping", &PingArgs{px.me, px.mins[px.me]}, reply)} ()
      select {
        case ok := <- rpc:
          if ok && reply.OK { //This should be ok assuming communciation is bidirectional
            px.PingLock.Lock()
            px.PingTimes[index] = time.Now()
            px.PingLock.Unlock()
            px.UpdateMins(index, reply.Done)
          }
        case <-time.After(PING_TIMEOUT):
      }
    }
 
    time.Sleep(PING_INTERVAL)
  }
  }()
  go func() {
  for true {
    if px.dead {
      return
    }
    
    reply := &LeaderReply{}
    px.DetermineLeader(&LeaderArgs{}, reply)
    
    px.LeaderLock.Lock()
    if px.Leader && reply.Leader != px.me {
      px.Leader = false;
    }

    if reply.Leader == px.me {
      px.Leader = true;
    }
    px.LeaderLock.Unlock()
    time.Sleep(PING_INTERVAL)
  }
  }()
}

func (px *Paxos) Ping(args *PingArgs, reply *PingReply) error {
  //TODO acquire lock
  px.PingLock.Lock()
  px.PingTimes[args.Me] = time.Now()
  px.PingLock.Unlock()
  reply.OK = true 
  px.UpdateMins(args.Me, args.Done) 
  return nil 
}

func (px *Paxos) DetermineLeader(args *LeaderArgs, reply *LeaderReply) error {
  now := time.Now()
  leader := -1
  for key, val := range px.PingTimes {
    if val.Add(LEADER_LEASE).After(now) && key > leader {
      leader = key
    } 
  }
  if leader < px.me {
    leader = px.me
  }
  reply.Leader = leader
  return nil
}

func (px *Paxos) Start(seq int, val interface{}) {
  px.LeaderLock.RLock()
  leader := px.Leader
  px.LeaderLock.RUnlock()
  if leader {
    go px.DoPaxos(seq, val)
  } else {
    go px.ForwardRequest(seq, val)
  }
}

func (px *Paxos) ForwardRequest(seq int, val interface{}) {
  for true {
    reply := &LeaderReply{}
    px.DetermineLeader(&LeaderArgs{}, reply)
    ok := call(px.peers[reply.Leader], "Paxos.ForwardedRequest", &ForwardedArgs{seq, val}, &ForwardedReply{})
    if ok { 
      return 
    }
    time.Sleep(10 * time.Millisecond)
  }
}

func (px *Paxos) ForwardedRequest(args *ForwardedArgs, reply *ForwardedReply) error {
  px.Start(args.Seq, args.Val)
  return nil
}

/*
proposer(v):
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all
*/
func (px *Paxos) DoPaxos(seq int, val interface{}) {
  instance := px.GetInstance(seq);
  instance.multi.Lock()
  for !px.dead  {
    px.instanceDataLock.RLock()
    if instance.decided {
      instance.multi.Unlock()
      px.instanceDataLock.RUnlock()
      return
    }
    px.instanceDataLock.RUnlock()
    // Choose and n that is unique and higher than anything seen before
    proposalNum := (time.Now().UnixNano() << 8) + int64(px.me)
    //numPrepareOks := 0
    //maxProposalNum := int64(-1)
    maxProposalVal := val
    /**
    for peerIndex, _ := range px.peers {
      args := PrepareArgs{Seq: seq, Num: proposalNum, Me: px.me}
      var reply PrepareReply
      ok := true
      rpc := make(chan bool, 1)
      if peerIndex == px.me {
        px.Prepare(&args, &reply)
        if ok && reply.Ok {
          numPrepareOks ++
          if reply.Num > maxProposalNum {
            maxProposalNum = reply.Num
            maxProposalVal = reply.Val
          }
        }
      } else {
        go func() { rpc <- call(px.peers[peerIndex], "Paxos.Prepare", &args, &reply) }()
        select {
          case ok = <- rpc:
            if ok && reply.Ok {
              numPrepareOks ++
              if reply.Num > maxProposalNum {
                maxProposalNum = reply.Num
                maxProposalVal = reply.Val
              }
            }
            px.UpdateMins(peerIndex, reply.Done)
          case <-time.After(PAXOS_TIMEOUT):
        }
      }
    }**/
    /*
    if prepare_ok(n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
    */
    numAcceptOks := 0
    if true { //numPrepareOks > px.majority {
      
      for peerIndex, _ := range px.peers {
        args := AcceptArgs{Seq: seq, Num: proposalNum, Val: maxProposalVal, Done: px.mins[px.me], Me: px.me}
        var reply AcceptReply
        ok := true
        rpc := make(chan bool, 1)
        if peerIndex == px.me {
          //println("Trying Accept")
          px.Accept(&args, &reply)
          //println("Accepted")
          if reply.Decided {
            instance.multi.Unlock()
            return
          }
          if ok && reply.Ok {
            numAcceptOks ++
          }
          px.UpdateMins(peerIndex, reply.Done)
        } else {
          go func() { rpc <- call(px.peers[peerIndex], "Paxos.Accept", &args, &reply) }()
          select {
            case ok = <- rpc:
              if reply.Decided {
                instance.multi.Unlock()
                return
              }
              if ok && reply.Ok {
                numAcceptOks ++
              }
              px.UpdateMins(peerIndex, reply.Done)
            case <-time.After(PAXOS_TIMEOUT):
          }
        }
      }
      if numAcceptOks > px.majority {
        px.SendDecides(seq, proposalNum, maxProposalVal)
        instance.multi.Unlock()
        return
      }
    }
    time.Sleep(10 * time.Millisecond)
  }
}

func (px *Paxos) UpdateMins(index int, newVal int) {
  px.minsLock.Lock()
  if px.mins[index] < newVal {
    px.mins[index] = newVal
  }
  px.minsLock.Unlock()
}

func (px *Paxos) UpdateMax(newVal int) {
  px.maxLock.Lock()
  if px.max < newVal {
    px.max = newVal
  }
  px.maxLock.Unlock()
}


func (px *Paxos) SendDecides(seq int, proposalNum int64, val interface{}) {
  decided := 0
  peers := make(map[int]string)
  for peerIndex, peer := range px.peers {
    peers[peerIndex] = peer
  }
  for decided <= px.majority {
    for peerIndex, _ := range peers {
      args := DecidedArgs{Seq: seq, Val: val, Num: proposalNum}
      var reply DecidedReply
      if peerIndex == px.me {
        px.Decided(&args, &reply)
        delete(peers, peerIndex)
        decided ++
      } else {
        ok := call(px.peers[peerIndex], "Paxos.Decided", &args, &reply)
        if ok {
          delete(peers, peerIndex)
          decided ++
        }
      }
    }
  }
}

func (px *Paxos) GetInstance(seq int) *Instance {
  px.instanceDataLock.Lock()
  if _,ok := px.instances[seq]; !ok {
    px.instances[seq] = &Instance{prepareNum: -1, acceptNum: -1, acceptVal: nil, decided: false}
  }
  px.instanceDataLock.Unlock()
  return px.instances[seq]
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.UpdateMins(args.Me, args.Done)
  px.UpdateMax(args.Seq)
  instance := px.GetInstance(args.Seq)
  px.instanceDataLock.Lock()
  if args.Num >= instance.prepareNum {
    instance.prepareNum = args.Num
    instance.acceptNum = args.Num
    instance.acceptVal = args.Val
    reply.Decided = instance.decided
    reply.Num = args.Num
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  px.instanceDataLock.Unlock()
  reply.Done = px.EvalDone()
  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.UpdateMax(args.Seq)

  instance := px.GetInstance(args.Seq)
  px.instanceDataLock.Lock()
  if !instance.decided {
    instance.acceptVal = args.Val
    instance.decided = true
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  px.instanceDataLock.Unlock()
  return nil
}


//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.UpdateMins(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.maxLock.Lock()
  defer px.maxLock.Unlock()
  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  return px.EvalDone() + 1
}

func (px *Paxos) EvalDone() int {
  px.minsLock.Lock()
  done := px.mins[0]
  for i := 1; i < len(px.mins); i ++ {
    done = min(px.mins[i], done)
  }
  if px.prevDone < done {
    px.DeleteTo(done)
  }
  px.prevDone = done
  px.minsLock.Unlock()
  return done
}

func (px* Paxos) DeleteTo(done int) {
  px.instanceDataLock.Lock()
  for key, _ := range px.instances {
    if key <= done {
      delete(px.instances, key)
    }
  }
  px.instanceDataLock.Unlock()
}


//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  instance := px.GetInstance(seq)
  px.instanceDataLock.RLock()
  defer px.instanceDataLock.RUnlock()
  return instance.decided, instance.acceptVal
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  // Your initialization code here.
  px.instances = make(map[int]*Instance)
  px.max = -1

  px.mins = make([]int,len(peers))
  for i,_ := range px.mins {
    px.mins[i] = -1
  }
  px.majority = len(px.peers)/2
  px.prevDone = -1
  px.PingTimes = make(map[int]time.Time)
  for index, _ := range px.peers {
    px.PingTimes[index] = time.Now()
  }
  if px.me + 1 == len(px.peers) {
    px.Leader = true
  } else {
    px.Leader = false
  }
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }
  go px.MultiPaxos()
  return px
}
