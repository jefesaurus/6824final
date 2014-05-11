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
import "strings"
import "strconv"

type Paxos struct {
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  db *DBClerk
  peers []string
  me int // index into peers[]
  majority int

  //instances map[int]*Instance
  multi_locks map[int]sync.Mutex

  //instancesLock sync.Mutex
  instanceDataLock sync.RWMutex

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
  PING_INTERVAL = time.Second * 2
  LEADER_LEASE = PING_INTERVAL * 5
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
      go func() { rpc <- call(server, "Paxos.Ping", &PingArgs{px.me, px.GetPeerMin(px.me)}, reply)} ()
      select {
        case ok := <-rpc:
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

func (px *Paxos) DoPaxos(seq int, val interface{}) {
  instance := px.GetInstance(seq);
  px.instanceDataLock.Unlock()
  if mu, ok := px.multi_lock[seq]; ok {
    mu.Lock()
  }
  fmt.Printf("%d, proposing\n", px.me)
  for !px.dead  {
    fmt.Printf("%d, rlocking\n", px.me)
    px.instanceDataLock.RLock()
    if instance.Decided {
      //instance.multi.Unlock()
    fmt.Printf("%d, runlocking\n", px.me)
      px.instanceDataLock.RUnlock()
      return
    }
    fmt.Printf("%d, runlocking\n", px.me)
    px.instanceDataLock.RUnlock()
    // Choose and n that is unique and higher than anything seen before
    proposalNum := (time.Now().UnixNano() << 8) + int64(px.me)
    maxProposalVal := val
    /*
    if prepare_ok(n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
    */
    numAcceptOks := 0
    if true { //numPrepareOks > px.majority {
      for peerIndex, _ := range px.peers {
        args := AcceptArgs{Seq: seq, Num: proposalNum, Val: maxProposalVal, Done: px.GetPeerMin(px.me), Me: px.me}
        var reply AcceptReply
        ok := true
        rpc := make(chan bool, 1)
        if peerIndex == px.me {
          px.Accept(&args, &reply)
          if reply.Decided {
            //instance.multi.Unlock()
            return
          }
          if ok && reply.Ok {
            numAcceptOks ++
          }
          px.UpdateMins(peerIndex, reply.Done)
        } else {
          go func() { rpc <- call(px.peers[peerIndex], "Paxos.Accept", &args, &reply) }()
          select {
            case ok = <-rpc:
              if reply.Decided {
                //instance.multi.Unlock()
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
        //instance.multi.Unlock()
        return
      }
    }
    time.Sleep(10 * time.Millisecond)
  }
}

func (px *Paxos) UpdateMins(index int, newVal int) {
  px.minsLock.Lock()
  current_min := px.GetPeerMin(index)
  if current_min < newVal {
    px.SetPeerMin(index, newVal)
  }
  px.minsLock.Unlock()
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
  fmt.Printf("%d, locking, getinst\n", px.me)
  px.instanceDataLock.Lock()

  inst := new(Instance)
  exists := px.db.GetStruct(METADATA, "instance" + strconv.Itoa(seq), inst)

  if !exists {
    inst = &Instance{PrepareNum: -1, AcceptNum: -1, AcceptVal: nil, Decided: false}
    px.db.PutStruct(METADATA, "instance" + strconv.Itoa(seq), inst)
  }
  return inst
}


func (px *Paxos) PutInstance(seq int, inst *Instance) {
  px.db.PutStruct(METADATA, "instance" + strconv.Itoa(seq), inst)
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.UpdateMins(args.Me, args.Done)
  px.UpdateMax(args.Seq)

  instance := px.GetInstance(args.Seq)
  fmt.Printf("%d, locked accept\n", px.me)
  if args.Num >= instance.PrepareNum {
    new_inst := new(Instance)
    new_inst.PrepareNum = args.Num
    new_inst.AcceptNum = args.Num
    new_inst.AcceptVal = args.Val
    px.PutInstance(args.Seq, new_inst)

    reply.Decided = instance.Decided
    reply.Num = args.Num
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  fmt.Printf("%d, unlocking accept\n", px.me)
  px.instanceDataLock.Unlock()
  reply.Done = px.EvalDone()
  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.UpdateMax(args.Seq)

  instance := px.GetInstance(args.Seq)
  fmt.Printf("%d, locked decided\n", px.me)
  if !instance.Decided {
    new_inst := new(Instance)
    new_inst.PrepareNum = instance.PrepareNum
    new_inst.AcceptNum = instance.AcceptNum
    new_inst.AcceptVal = args.Val
    new_inst.Decided = true
    px.PutInstance(args.Seq, new_inst)

    reply.Ok = true
  } else {
    reply.Ok = false
  }
  fmt.Printf("%d, unlocking decided\n", px.me)
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
  done := px.GetPeerMin(0)
  for i := 1; i < len(px.peers); i ++ {
    done = min(px.GetPeerMin(i), done)
  }
  if px.prevDone < done {
    px.DeleteTo(done)
  }
  px.prevDone = done
  px.minsLock.Unlock()
  return done
}

func (px* Paxos) DeleteTo(done int) {
  fmt.Printf("%d, locking deleteto\n", px.me)
  px.instanceDataLock.Lock()
  /*
  for key, _ := range px.instances {
    if key <= done {
      delete(px.instances, key)
    }
  }
  */
  fmt.Printf("%d, unlocking delete to\n", px.me)
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
  fmt.Printf("%d, locking status\n", px.me)
  instance := px.GetInstance(seq)
  fmt.Printf("%d, locked status\n", px.me)
  defer px.instanceDataLock.Unlock()
  return instance.Decided, instance.AcceptVal
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
const db_path = "/tmp/pxdb/"

func MakeFromDB(me string) {
  px := &Paxos{}
  px.db = GetDatabase(db_path + me)

  did_restore := px.TryRestoreFromDisk()
  if !did_restore {
    log.Fatal("RESTORE FROM DB FAILED")
  }
}

func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  path_parts := strings.Split(peers[me], "/")
  px_db_path := db_path + path_parts[len(path_parts)-1]
  os.Remove(px_db_path)
  px.db = GetDatabase(px_db_path)
  px.FreshStart(peers, me)
  FinishMake(px, rpcs)
  return px
}

func FinishMake(px *Paxos, rpcs *rpc.Server) *Paxos {
  // Your initialization code here.

  px.majority = len(px.peers)/2
  px.prevDone = -1
  px.PingTimes = make(map[int]time.Time)
  px.multi_locks = make(map[int]sync.Mutex)
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
    os.Remove(px.peers[px.me]) // only needed for "unix"
    l, e := net.Listen("unix", px.peers[px.me]);
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
          fmt.Printf("Paxos(%v) accept: %v\n", px.me, err.Error())
        }
      }
    }()
  }
  go px.MultiPaxos()
  return px
}



// DB INTERFACE
func DeleteDB() {
  err := os.RemoveAll(db_path)
  if err != nil {
    log.Fatal(err)
  }
}

func (px *Paxos) GetPeerMin(peer_index int) int {
  peer_min_key := fmt.Sprintf("mins-%s", px.peers[peer_index])
  peer_min, exists := px.db.GetInt(METADATA, peer_min_key)
  if !exists {
    log.Fatal("No record of min for peer: %d", px.peers[peer_index])
  }
  return peer_min
}

func (px *Paxos) SetPeerMin(peer_index int, min int) {
  peer_min_key := fmt.Sprintf("mins-%s", px.peers[peer_index])
  px.db.PutInt(METADATA, peer_min_key, min)
}

func (px *Paxos) TryRestoreFromDisk() bool {
  me, me_exists := px.db.GetInt(METADATA, "me")
  if !me_exists {
    return false
  }

  // First get list of peers
  // me is 0
  stored_peers, peers_exists := px.db.GetStringList(METADATA, "peers")
  if !peers_exists {
    return false
  }
  for _, peer := range stored_peers {
    println(peer)
  }

  // Make sure it has the max
  _, max_exists := px.db.GetInt(METADATA, "max");
  if !max_exists {
    return false
  }

  // Make sure it has the peer mins
  for _, peer := range stored_peers {
    peer_min_key := fmt.Sprintf("mins-%s", peer)
    _, exists := px.db.GetInt(METADATA, peer_min_key)
    if !exists {
      return false
    }
  }

  // Commit some statics to local state:
  px.me = me
  px.peers = stored_peers
  return true
}


func (px *Paxos) FreshStart(peers []string, me int) {
  // Put my index
  px.db.PutInt(METADATA, "me", me)
  px.me = me

  // Enter peers
  px.db.PutStringList(METADATA, "peers", peers)
  px.peers = peers

  // Init peer mins in DB
  for _, peer:= range peers {
    peer_min_key := fmt.Sprintf("mins-%s", peer)
    px.db.PutInt(METADATA, peer_min_key, -1)
  }

  // Init max in DB
  px.SetMax(-1)
}

func (px *Paxos) SetMax(max int) {
  px.db.PutInt(METADATA, "max", max)
}

func (px *Paxos) Max() int {
  current_max, success := px.db.GetInt(METADATA, "max")
  if success {
    return current_max
  } else {
    log.Fatal("Error getting max")
    return -1
  }
}

func (px *Paxos) UpdateMax(newVal int) {
  current_max, success := px.db.GetInt(METADATA, "max")
  if success {
    if current_max < newVal {
      px.db.PutInt(METADATA, "max", newVal)
    }
  } else {
    log.Fatal("Error getting max")
  }
}
