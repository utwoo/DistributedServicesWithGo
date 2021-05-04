package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"net"
	"sync"
	"time"
	"utwoo.com/DistributedServicesWithGo/internal/auth"
	"utwoo.com/DistributedServicesWithGo/internal/discovery"
	"utwoo.com/DistributedServicesWithGo/internal/log"
	"utwoo.com/DistributedServicesWithGo/internal/server"
)

// An Agent runs on every service instance, setting up and connecting all the
// different components. The struct references each component (log, server,
// membership, replicator) that the Agent manages.
// c8. Here we’ve added the mux cmux.CMux field, changed the log to a DistributedLog,
// and removed the replicator.
type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// The Agent sets up the components so its Config comprises the components’
// parameters to pass them through to the components.
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool //c8.bootstrapping the Raft cluster
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// NewAgent creates an Agent and runs a set of methods to set up and run the
// agent’s components. After we run New(), we expect to have a running, functioning service.
func NewAgent(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	// tell our mux to serve connections.
	go a.serve()

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// setupMux() creates a listener on our RPC address that’ll accept both Raft and
// gRPC connections and then creates the mux with the listener. The mux will
// accept connections on that listener and match connections based on your
// configured rules.
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(listener)
	return nil
}

func (a *Agent) setupLog() error {
	// We configure the mux that matches Raft connections.
	// We identify Raft connections by reading one byte and checking that the byte
	// matches the byte we set up our outgoing Raft connections.
	// If the mux matches this rule, it will pass the connection to the raftLn listener
	// for Raft to handle the connection.
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})

	// We configure the distributed log’s Raft to use our multiplexed listener and
	// then configure and create the distributed log.
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(a.Config.DataDir, logConfig)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	authorizer, err := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	if err != nil {
		return err
	}
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		serverCredentials := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(serverCredentials))
	}
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// Because we’ve multiplexed two connection types (Raft and gRPC) and we
	// added a matcher for the Raft connections, we know all other connections
	// must be gRPC connections. We use cmux.Any() because it matches any connections
	// Then we tell our gRPC server to serve on the multiplexed listener.
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

// setupMembership() sets up a Replicator with the gRPC dial options needed to connect
// to other servers and a client so the replicator can connect to other servers,
// consume their data, and produce a copy of the data to the local server. Then
// we create a Membership passing in the replicator and its handler to notify the
// replicator when servers join and leave the cluster.
// c.8: Our DistributedLog handles coordinated replication, thanks to Raft, so we don’t
// need the Replicator anymore. Now we need the Membership to tell the DistributedLog
// when servers join or leave the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.NewMembership(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// This ensures that the agent will shut down once even if people call Shutdown()
// multiple times. Then we shut down the agent and its components by:
// • Leaving the membership so that other servers will see that this server
//	 has left the cluster and so that this server doesn’t receive discovery events
//	 anymore;
// • Closing the replicator so it doesn’t continue to replicate;
// • Gracefully stopping the server, which stops the server from accepting new
//	 connections and blocks until all the pending RPCs have finished; and
// • Closing the log.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	//shutdown := []func() error{
	//	a.membership.Leave,
	//	a.replicator.Close,
	//	func() error {
	//		a.server.GracefulStop()
	//		return nil
	//	},
	//	a.log.Close,
	//}
	//for _, fn := range shutdown {
	//	if err := fn(); err != nil {
	//		return err
	//	}
	//}

	a.membership.Leave()
	func() error {
		a.server.GracefulStop()
		return nil
	}()
	a.log.Close()
	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
