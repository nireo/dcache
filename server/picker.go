package server

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type Picker struct {
	sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	curr      uint64
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(ResolverName, &Picker{}, base.Config{}),
	)
}

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.Lock()
	defer p.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}

		followers = append(followers, sc)
	}

	p.followers = followers

	return p
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.RLock()
	defer p.RUnlock()

	var res balancer.PickResult
	if strings.Contains(info.FullMethodName, "Set") || len(p.followers) == 0 {
		res.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Get") {
		res.SubConn = p.nextFollower()
	}

	if res.SubConn == nil {
		return res, balancer.ErrNoSubConnAvailable
	}

	return res, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.curr, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)

	return p.followers[idx]
}
