package discovery

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"net"
)

// The Handler represents some component in our service that needs to know
// when a server joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// New function to define the configuration type and set up Serf:
func NewMembership(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// Membership is our type wrapping Serf to provide discovery and cluster member-
// ship to our service. Users will call New() to create a Membership with the required
// configuration and event handler.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// setupSerf() creates and configures a Serf instance and starts the eventsHandler()
// goroutine to handle Serf’s events.
// Serf has a lot of configurable parameters, but the five parameters you’ll typically use are:
// • NodeName—the node name acts as the node’s unique identifier across the
//   Serf cluster. If you don’t set the node name, Serf uses the hostname.
// • BindAddr and BindPort—Serf listens on this address and port for gossiping.
// • Tags—Serf shares these tags to the other nodes in the cluster and should
//   use these tags for simple data that informs the cluster how to handle this
//   node. For example, Consul shares each node’s RPC address with Serf
//	 tags, and once they know each other’s RPC address, they can make RPCs
//	 to each other. Consul shares whether the node is a voter or non-voter,
//	 which changes the node’s role in the Raft cluster. We’ll talk about this
//	 more in the next chapter when we use Raft to build consensus in our
//	 cluster. In our code, similar to Consul, we’ll share each node’s user-con-
//	 figured RPC address with a Serf tag so the nodes know which addresses
//	 to send their RPCs.
// • EventCh—the event channel is how you’ll receive Serf’s events when a node
//	 joins or leaves the cluster. If you want a snapshot of the members at any
//	 point in time, you can call Serf’s Members() method.
// • StartJoinAddrs—when you have an existing cluster and you create a new node
//	 that you want to add to that cluster, you need to point your new node to
//	 at least one of the nodes now in the cluster. After the new node connects
//	 to one of those nodes in the existing cluster, it’ll learn about the rest of
//	 the nodes, and vice versa (the existing nodes learn about the new node).
//	 The StartJoinAddrs field is how you configure new nodes to join an existing
//	 cluster. You set the field to the addresses of nodes in the cluster, and
//	 Serf’s gossip protocol takes care of the rest to join your node to the cluster.
//	 In a production environment, specify at least three addresses to make
//	 your cluster resilient to one or two node failures or a disrupted network.
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Config.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		if _, err := m.serf.Join(m.StartJoinAddrs, true); err != nil {
			return err
		}
	}
	return nil
}

// The eventHandler() runs in a loop reading events sent by Serf into the events
// channel, handling each incoming event according to the event’s type. When
// a node joins or leaves the cluster, Serf sends an event to all nodes, including
// the node that joined or left the cluster. We check whether the node we got an
// event for is the local server so the server doesn’t act on itself—we don’t want
// the server to try and replicate itself, for example.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	)
	if err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	err := m.handler.Leave(
		member.Name)
	if err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether the given Serf member is the local member by checking the members’ names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns a point-in-time snapshot of the cluster’s Serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells this member to leave the Serf cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs the given error and message.
// Raft will error and return ErrNotLeader when you try to change the cluster on
// non-leader nodes. In our service discovery code we log all handler errors as
// critical, but if the node is a non-leader, then we should expect these errors
// and not log them.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
