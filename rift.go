package main

import (
	"git.code.oa.com/trpc-go/trpc-go/log"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"net/rpc"
	"sync"
	"time"
)

const (
	pprofAddr string = ":7890"
)

func StartHTTPDebuger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

type Rift struct {
	mu                 sync.Mutex
	wg                 sync.WaitGroup
	state              State
	term               uint32
	peerIds            map[uint32]string
	clients            map[uint32]*RiftClient
	listener           net.Listener
	svr                *rpc.Server
	id                 uint32
	voteFor            uint32
	electionResetEvent time.Time
}

func NewRift(id, term uint32, peers map[uint32]string) (r *Rift, err error) {
	r = &Rift{id: id, term: term, peerIds: peers}
	r.voteFor = 0
	r.clients = make(map[uint32]*RiftClient)
	return
}

func (r *Rift) disconnectPeer(peerId uint32) error {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	defer r.mu.Unlock()
	if r.clients[peerId] != nil {
		err := r.clients[peerId].Close()
		r.clients[peerId] = nil
		return err
	}
	return nil
}

func (r *Rift) connectPeer(peerId uint32, addr net.Addr) error {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	defer r.mu.Unlock()
	if r.id == peerId {
		return nil
	}
	var err error
	r.clients[peerId], err = DialRiftService(addr.Network(), addr.String())
	return err
}

func (r *Rift) serve() error {
	var err error
	r.svr, err = RegisterRiftService("raft", r)
	if err != nil {
		return err
	}
	r.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	//log.Infoff("[%v] listening at %s", r.id, r.listener.Addr())

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				select {
				default:
					log.Fatal("accept error:", err)
				}
			}
			r.wg.Add(1)
			go func() {
				r.svr.ServeConn(conn)
				r.wg.Done()
			}()
		}
	}()
	return nil
}
func getLog() {

}

func (r *Rift) electionTimer() {
	timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	termStarted := r.term
	r.electionResetEvent = time.Now()
	r.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		//log.Infoff("%v：等锁中", r.id)
		r.mu.Lock()
		//log.Infoff("%v：拿到啦", r.id)
		if r.state != Candidate && r.state != Follower {
			r.mu.Unlock()
			return
		}

		if termStarted != r.term {
			r.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(r.electionResetEvent); elapsed >= timeoutDuration {
			go r.startElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Rift) toFollower(term uint32) {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	//log.Infoff("r(%v) toFollower", r.id)
	defer r.mu.Unlock()
	r.term = term
	r.state = Follower
	r.electionResetEvent = time.Now()
	go r.electionTimer()
	r.voteFor = 0
}

func (r *Rift) toCandiDate(term uint32) {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	//log.Infoff("r(%v) toCandiDate", r.id)
	defer r.mu.Unlock()
	r.term = term
	r.state = Candidate
	r.voteFor = r.id
}

func (r *Rift) toLeader(term uint32) {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	defer r.mu.Unlock()
	r.state = Leader
	//log.Infoff("r(%v) toLeader", r.id)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			r.heartBeatSend()
			<-ticker.C

			//log.Infoff("%v：等锁中", r.id)
			r.mu.Lock()
			//log.Infoff("%v：拿到啦", r.id)
			if r.state != Leader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

func (r *Rift) Vote(req *VoteReq, rsp *VoteRsp) error {
	//log.Infoff("%v：等锁中", r.id)
	r.mu.Lock()
	//log.Infoff("%v：拿到啦", r.id)
	defer r.mu.Unlock()
	defer log.Infof("r(%v) VoteRecv:%+v,Rsp:%+v\n", r.id, req, rsp)
	if req.Term < r.term {
		rsp.Grant = false
		rsp.Term = r.term
		return nil
	}
	if r.voteFor != 0 && r.voteFor != req.Id {
		rsp.Grant = false
		rsp.Term = r.term
		return nil
	}
	rsp.Grant = true
	rsp.Term = r.term
	r.voteFor = req.Id
	r.term = req.Term

	return nil
}

func (r *Rift) HeartBeatRec(req *HeartBeatReq, rsp *HeartBeatRsp) error {
	//log.Infoff("HeartBeatRec:%+v\n", req)
	if r.term <= req.Term {
		r.toFollower(req.Term)
	}
	rsp.Term = r.term
	return nil
}

func (r *Rift) heartBeatSend() {
	//return nil
	for id, _ := range r.peerIds {
		if id == r.id {
			continue
		}
		go func(id uint32) {
			req := &HeartBeatReq{
				Term: r.term,
				Id:   r.id,
			}
			rsp := &HeartBeatRsp{}
			if err := r.clients[id].HeartBeatSend(req, rsp); err == nil {
				//log.Infoff("%v：等锁中", r.id)
				r.mu.Lock()
				//log.Infoff("%v：拿到啦", r.id)
				defer r.mu.Unlock()
				if rsp.Term > r.term {
					r.toFollower(rsp.Term)
				}
				return
			}
		}(id)
	}
}

func (r *Rift) startElection() {
	voteCount := 1
	var mu sync.Mutex
	//var wg sync.WaitGroup
	r.term++
	r.voteFor = r.id
	for id, _ := range r.peerIds {
		if id == r.id {
			continue
		}
		go func(id uint32) {
			req := &VoteReq{
				Term: r.term,
				Id:   r.id,
			}
			rsp := &VoteRsp{}
			if err := r.clients[id].Election(req, rsp); err == nil {
				mu.Lock()
				defer mu.Unlock()
				if rsp.Grant {
					voteCount++
					if voteCount*2 > len(r.peerIds)+1 {
						if r.state == Candidate {
							r.toLeader(r.term)
						}
						return
					}
				}
				if rsp.Term > r.term {
					r.toFollower(rsp.Term)
				}
			}
		}(id)
	}
	go r.electionTimer()
}
func RaftFactor(count int) {
	peersAddr := make(map[uint32]string)
	for i := 1; i <= count; i++ {
		peersAddr[uint32(i)] = ""
	}
	peers := make(map[uint32]*Rift)
	getLeader := func() uint32 {
		var leader uint32 = 0
		//for _, rift := range peers {
		//	rift.mu.Lock()
		//}
		for _, rift := range peers {
			if rift.state == Leader {
				leader = rift.id
			}
		}
		//for _, rift := range peers {
		//	rift.mu.Unlock()
		//}
		return leader

	}
	for i, _ := range peersAddr {
		var err error
		if peers[i], err = NewRift(i, 1, peersAddr); err != nil {
			panic(err)
		}
		if err = peers[i].serve(); err != nil {
			panic(err)
		}
	}
	for i, _ := range peers {
		for u, _ := range peers {
			err := peers[i].connectPeer(u, peers[u].listener.Addr())
			if err != nil {
				panic(err)
			}
		}
	}
	//for i, _ := range peers {
	//	peers[i].electionTimer()
	//}
	for i, _ := range peers {
		peers[i].toCandiDate(1)
		go peers[i].electionTimer()
	}
	t := time.NewTicker(time.Millisecond * 600)
	flag := true
	for {
		<-t.C
		//continue
		leader := getLeader()
		if leader == 0 {
			continue
		}

		log.Infof("###########SnapHot:##########\nleader:")
		if flag {
			for _, rift := range peers {
				err := rift.disconnectPeer(leader)
				if err != nil {
					panic(err)
				}
			}
			//log.Infoff("disconnect:%v\n", leader)
			flag = false
		} else {
			for _, rift := range peers {
				err := rift.connectPeer(leader, peers[leader].listener.Addr())
				if err != nil {
					panic(err)
				}
			}
			//log.Infoff("connect:%v\n", leader)
			flag = true
		}

	}
}

func main() {
	StartHTTPDebuger()
	RaftFactor(3)
}
