package loadbalance

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"sync"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
)

// Resolver is the type we’ll implement into gRPC’s resolver.Builder and resolver.Resolver
// interfaces. The clientConn connection is the user’s client connection and gRPC
// passes it to the resolver for the resolver to update with the servers it discovers.
// The resolverConn is the resolver’s own client connection to the server so it can
// call GetServers() and get the servers
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// *** Resolver ***
var _ resolver.Builder = (*Resolver)(nil)

const Name = "proglog"

// Build receives the data needed to build a resolver that can discover the
// servers (like the target address) and the client connection the resolver will
// update with the servers it discovers. Build() sets up a client connection to
// our server so the resolver can call the GetServers() API.
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	// Services can specify how clients should balance their calls to the service by
	// updating the state with a service config. We update the state with a service
	// config that specifies to use the “proglog” load balancer
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}"`, Name),
	)
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// Scheme returns the resolver’s scheme identifier. When you call grpc.Dial,
// gRPC parses out the scheme from the target address you gave it and tries
// to find a resolver that matches, defaulting to its DNS resolver. For our
// resolver, you’ll format the target address like this: proglog://your-service-address.
func (r *Resolver) Scheme() string {
	return Name
}

// We register this resolver with gRPC in init() so gRPC knows about this resolver
// when it’s looking for resolvers that match the target’s scheme.
func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

// ResolveNow gRPC calls to resolve the target, discover the servers, and update the client
// connection with the servers. How your resolver will discover the servers
// depends on your resolver and the service you’re working with.
// We create a gRPC client for our service and call the GetServers() API to get the cluster’s servers.
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("fialed to resolve server", zap.Error(err))
		return
	}
	// You update the state with a slice of resolver.Address to inform the load balancer
	// what servers it can choose from.
	// A resolver.Address has three fields:
	//• Addr (required)—the address of the server to connect to.
	//• Attributes (optional but useful)—a map containing any data that’s useful
	//	for the load balancer. We’ll tell the picker what server is the leader and
	//	what servers are followers with this field.
	//• ServerName (optional and you likely don’t need to set)—the name used as
	//	the transport certificate authority for the address, instead of the hostname
	//	taken from the Dial target string.
	var addresses []resolver.Address
	for _, server := range res.Servers {
		addresses = append(addresses, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	// After we’ve discovered the servers, we update the client connection by calling
	// UpdateState() with the resolver.Address’s. We set up the addresses with the data in
	// the api.Server’s. gRPC may call ResolveNow() concurrently, so we use a mutex to
	// protect access across goroutines.
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addresses,
		ServiceConfig: r.serviceConfig,
	})
}

// Close closes the resolver. In our resolver, we close the connection to our
// server created in Build().
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}
