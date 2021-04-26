package server

import (
	"context"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
	"utwoo.com/DistributedServicesWithGo/internal/log"
)

// TestServer(*testing.T) defines our list of test cases and then runs a subtest for each case
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		*testing.T,
		api.LogClient,
		*Config,
	){
		//"produce/consume a message to/from the log succeeeds":
		//testProduceConsume,
		//"produce/consume stream succeeds":
		//testProduceConsumeStream,
		"consume past log boundary fails": testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

// setupTest(*testing.T, func(*Config)) is a helper function to set up each test case. Our
// test setup begins by creating a listener on the local network address that our
// server will run on. The 0 port is useful for when we don’t care what port we
// use since 0 will automatically assign us a free port. We then make an insecure
// connection to our listener and, with it, a client we’ll use to hit our server with.
// Next we create our server and start serving requests in a goroutine because
// the Serve method is a blocking call, and if we didn’t run it in a goroutine our
// tests further down would never run.
func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, config *Config, teardown func()) {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg := &Config{CommitLog: clog}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		clog.Remove()
	}
}

// testProduceConsume(*testing.T,api.LogClient,*Config) tests that producing and consuming
// works by using our client and server to produce a record to the log, consume
// it back, and then check that the record we sent is the same one we got back.
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// testConsumePastBoundary(*testing.T,api.LogClient, *Config) tests that our server responds
// with an api.ErrOffsetOutOfRange() error when a client tries to consume beyond the
// log’s boundaries.
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		})
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset + 1,
		})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

// testProduceConsumeStream(*testing.T, api.LogClient, *Config) is the streaming counterpart
// to testProduceConsume() , testing that we can produce and consume through
// streams.
func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		produceStream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err := produceStream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := produceStream.Recv()
			require.NoError(t, err)
			if res.Offset != record.Offset {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	{
		consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := consumeStream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{Value: record.Value, Offset: uint64(i)})
		}
	}
}
