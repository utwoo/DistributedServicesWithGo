package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
	"utwoo.com/DistributedServicesWithGo/internal/auth"
	"utwoo.com/DistributedServicesWithGo/internal/config"
	"utwoo.com/DistributedServicesWithGo/internal/log"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

// When a test file implements TestMain(m *testing.M), Go will call TestMain(m) instead
// of running the tests directly. TestMain() gives us a place for setup that applies
// to all tests in that file, like enabling our debug output. Flag parsing has to
// go in TestMain() instead of init(), otherwise Go can’t define the flag and your code
// will error and exit.
func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

// TestServer(*testing.T) defines our list of test cases and then runs a subtest for each case
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized fails":                                  testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
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
func setupTest(t *testing.T, fn func(*Config)) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// We’re creating two clients: a superuser 9 client called root who’s permitted to
	// produce and consume, and a nobody 10 client who isn’t permitted to do any-
	// thing. Because the code for creating both clients is the same (aside from which
	// cert and key they’re configured with), we’ve refactored the client creation code
	// into a newClient(crtPath, keyPath string) helper function. Our server now takes in an
	// Authorizer instance that the server will defer its authorization logic to. And we
	// pass both our root and nobody clients to the test functions so they can use
	// whatever client they need based on whether they’re testing how the server
	// works with an authorized or unauthorized client.
	newClient := func(crtPath, keyPath string) (api.LogClient, *grpc.ClientConn, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCredentials := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCredentials)}
		conn, err := grpc.Dial(listener.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return client, conn, opts
	}

	rootClient, rootConn, _ := newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyClient, nobodyConn, _ := newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// we’re parsing the server’s cert and key, which we then use to
	// configure the server’s TLS credentials. We then pass those credentials as a
	// gRPC server option to our NewGRPCServer() function so it can create our gRPC
	// server with that option. gRPC server options are how you enable features in
	// gRPC servers. We’re setting the credentials for the server connections in this
	// case, but there are plenty of other server options to configure connection
	// timeouts, keep alive policies, and so on.

	/* (TLS only in server)
	//serverTLSConfig, err := cfg.SetupTLSConfig(cfg.TLSConfig{
	//	CertFile:      cfg.ServerCertFile,
	//	KeyFile:       cfg.ServerKeyFile,
	//	CAFile:        cfg.CAFile,
	//	ServerAddress: listener.Addr().String(),
	//})
	*/

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
		Server:        true,
	})

	require.NoError(t, err)
	serverCredentials := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Update your test server’s configuration to pass in an authorizer.
	authorizer, _ := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	// Sets up and starts the telemetry exporter to write to two files.
	// Each test gets its own separate trace and metrics files so we can see each test’s requests.
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCredentials))
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		listener.Close()
		// We sleep for 1.5 seconds to give the telemetry exporter enough time to flush
		// its data to disk. Then we stop and close the exporter.
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

// testProduceConsume(*testing.T,api.LogClient,*Config) tests that producing and consuming
// works by using our client and server to produce a record to the log, consume
// it back, and then check that the record we sent is the same one we got back.
func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
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
func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
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
func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
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

// We use the nobody client, which isn’t permitted to do anything.
// We try to use the client to produce and consume, just as we did in the suc-
// cessful test case. Since our client isn’t authorized, we want our server to deny
// the client, which we verify by checking the code on the returned error.
func testUnauthorized(t *testing.T, _ api.LogClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := nobodyClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	wantCode := codes.PermissionDenied
	gotCode := status.Code(err)
	if gotCode != wantCode {
		t.Fatalf("produce got code: %d, want: %d", gotCode, wantCode)
	}

	consume, err := nobodyClient.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode = status.Code(err)
	wantCode = codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("consume got code: %d, want code: %d", gotCode, wantCode)
	}
}
