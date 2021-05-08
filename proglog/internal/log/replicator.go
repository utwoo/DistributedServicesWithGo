package log

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
	api "utwoo.com/proglog/api/v1"
)

// The replicator connects to other servers with the gRPC client, and we need
// to configure the client so it can authenticate with the servers. The clientOptions
// field is how we pass in the options to configure the client. The servers field
// is a map of server addresses to a channel, which the replicator uses to stop
// replicating from a server when the server fails or leaves the cluster. The
// replicator calls the produce function to save a copy of the messages it con-
// sumes from the other servers.
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Join method adds the given server address to the list of
// servers to replicate and kicks off the add goroutine to run the actual replication logic.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replication so skip
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	// Create a client and open up a stream to consume all logs on the server.
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// The loop consumes the logs from the discovered server in a stream and then
	// produces to the local server to save a copy. We replicate messages from the
	// other server until that server fails or leaves the cluster and the replicator
	// closes the channel for that server, which breaks the loop and ends the replicate()
	// goroutine. The replicator closes the channel when Serf receives an event
	// saying that the other server left the cluster, and then this server calls the
	// Leave() method that we’re about to add.
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave method handles the server leaving the cluster by
// removing the server from the list of servers to replicate and closes the server’s
// associated channel. Closing the channel signals to the receiver in the replicate()
// goroutine to stop replicating from that server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// We use this init() helper to lazily initialize the server map. You should use lazy
// initialization to give your structs a useful zero value because having a useful
// zero value reduces the API’s size and complexity while maintaining the same
// functionality. Without a useful zero value, we’d either have to export a repli-
// cator constructor function for the user to call or export the servers field on
// the replicator struct for the user to set—making more API for the user to learn
// and then requiring them to write more code before they can use our struct.
// https://dave.cheney.net/2013/01/19/what-is-the-zero-value-and-why-is-it-useful
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close closes the replicator so it doesn’t replicate new servers that join the
// cluster and it stops replicating existing servers by causing the replicate() goroutines to return.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	close(r.close)
	return nil
}

// handle errors
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
