package loadbalance

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"net"
	"testing"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
	"utwoo.com/DistributedServicesWithGo/internal/config"
	"utwoo.com/DistributedServicesWithGo/internal/server"
)

// getServers implements GetServerers, whose job is to return a known server set
// for the resolver to find.
type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		},
		{
			Id:      "follower",
			RpcAddr: "localhost:9002",
		},
	}, nil
}

// clientConn implements resolver.ClientConn, and its job is to
// keep a reference to the state the resolver updated it with so that we can verify
// that the resolver updates the client connection with the correct data.
type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) {
	c.state = state
}
func (c *clientConn) ReportError(err error)               {}
func (c *clientConn) NewAddress(addrs []resolver.Address) {}
func (c *clientConn) NewServiceConfig(config string)      {}
func (c *clientConn) ParseServiceConfig(
	config string,
) *serviceconfig.ParseResult {
	return nil
}

func TestResolver(t *testing.T) {
	// This code sets up a server for our test resolver to try and discover some servers from.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCredential := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCredential))
	require.NoError(t, err)

	go srv.Serve(listener)

	// This code creates and builds the test resolver and configures its target end-
	// point to point to the server we set up in the previous snippet. The resolver
	// will call GetServers() to resolve the servers and update the client connection
	// with the servers’ addresses.
	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	clientCredential := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCredential,
	}
	r := Resolver{}
	r.Build(
		resolver.Target{Endpoint: listener.Addr().String()},
		conn,
		opts,
	)
	require.NoError(t, err)

	// We check that the resolver updated the client connection with the servers
	// and data we expected. We wanted the resolver to find two servers and mark
	// the 9001 server as the leader.
	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr:       "localhost:9001",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr:       "localhost:9002",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}
