package loadbalance

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"strings"
	"sync"
	"sync/atomic"
)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

// Though pickers handle routing the calls, which we’d traditionally consider
// handling the balancing, gRPC has a balancer type that takes input from gRPC,
// manages subconnections, and collects and aggregates connectivity states.
// gRPC provides a base balancer; you probably don’t need to write your own.
func init() {
	balancer.Register(base.NewBalancerBuilder(Name, &Picker{}, base.Config{}))
}

var _ base.PickerBuilder = (*Picker)(nil)

// Pickers use the builder pattern just like resolvers. gRPC passes a map of
// subconnections with information about those subconnections to Build() to set
// up the picker—behind the scenes, gRPC connected to the addresses that our
// resolver discovered. Our picker will route consume RPCs to follower servers
// and produce RPCs to the leader server. The address attributes help us differ-
// entiate the servers.
func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

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

// Pickers have one method: Pick(balancer.PickInfo). gRPC gives Pick() a balancer.PickInfo
// containing the RPC’s name and context to help the picker know what subcon-
// nection to pick. If you have header metadata, you can read it from the context.
// Pick() returns a balancer.PickResult with the subconnection to handle the call.
// Optionally, you can set a Done callback on the result that gRPC calls when
// the RPC completes. The callback tells you the RPC’s error, trailer metadata,
// and whether there were bytes sent and received to and from the server.
var _ balancer.Picker = (*Picker)(nil)

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result balancer.PickResult
	// We look at the RPC’s method name to know whether the call is an append or
	// consume call, and if we should pick a leader subconnection or a follower
	// subconnection. We balance the consume calls across the followers with the
	// round-robin algorithm.
	if strings.Contains(info.FullMethodName, "Produce") || len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower()
	}

	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	length := uint64(len(p.followers))
	idx := int(cur % length)
	return p.followers[idx]
}
