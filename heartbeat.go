package main

import "net/rpc"

type HeartBeatReq struct {
	RequestID string
	Term      uint32
	Id        uint32
}

type HeartBeatRsp struct {
	Term uint32
}

type VoteReq struct {
	RequestID string
	Term      uint32
	Id        uint32
}

type VoteRsp struct {
	Term  uint32
	Grant bool
}

type RiftSvr interface {
	HeartBeatRec(req *HeartBeatReq, rsp *HeartBeatRsp) error
	Vote(req *VoteReq, rsp *VoteRsp) error
}

type RiftClient struct {
	*rpc.Client
}

//var _ HeartBeatInterface = (*HelloServiceClient)(nil)

func DialRiftService(network, address string) (*RiftClient, error) {
	c, err := rpc.Dial(network, address)
	if err != nil {
		panic(err)
		return nil, err
	}
	return &RiftClient{Client: c}, nil
}

func (p *RiftClient) HeartBeatSend(req *HeartBeatReq, rsp *HeartBeatRsp) error {
	if p != nil {
		return p.Client.Call("raft.HeartBeatRec", req, rsp)
	}
	return nil
}

func (p *RiftClient) Election(req *VoteReq, rsp *VoteRsp) error {
	if p != nil {
		return p.Client.Call("raft.Vote", req, rsp)
	}
	return nil

}

func RegisterRiftService(name string, svr RiftSvr) (*rpc.Server, error) {
	s := rpc.NewServer()
	if err := s.RegisterName(name, svr); err != nil {
		return nil, err
	}
	return s, nil

}
