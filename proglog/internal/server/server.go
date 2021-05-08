package server

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	api "utwoo.com/proglog/api/v1"
)

type Config struct {
	CommitLog   CommitLog
	Authorizer  Authorizer
	GetServerer GetServerer
}

// Authorizer we can switch out the authorization implementation
type Authorizer interface {
	Authorize(subject, object, action string) error
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

// The constants match the values we in our ACL policy table,
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

// We want to pass in a log implementation based on our needs at the time.
// We can make this possible by having our service depend on a log interface rather than on a concrete
// type. That way, the service can use any log implementation that satisfies the log interface.
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// Produce and Consume methods handle the requests made by clients
// to produce and consume to the server’s log.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// checking whether the client (identified by the cert’s subject) is authorized to produce
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// checking whether the client (identified by the cert’s subject) is authorized to consume
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream implements a bidirectional streaming
// RPC so the client can stream data into the server’s log and the server can tell
// the client whether each request succeeded.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream implements a server-side streaming RPC so the
// client can tell the server where in the log to read records, and then the server
// will stream every record that follows—even records that aren’t in the log yet!
// When the server reaches the end of the log, the server will wait until someone
// appends a record to the log and then continue streaming records to the client.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// These two snippets enable us to inject different structs that can get servers.
// We don’t want to add the GetServers() method to our CommitLog interface because
// a non-distributed log like our Log type doesn’t know about servers. So we
// made a new interface whose sole method GetServers() matches DistributedLog.Get-
// Servers. When we update the end-to-end tests in the agent package, we’ll set
// our DistributedLog on the config as both the CommitLog and the GetServerer—which
// our new server endpoint wraps with error handling.
func (s *grpcServer) GetServers(ctx context.Context, request *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}

func newgrpcServer(config *Config) (*grpcServer, error) {
	return &grpcServer{Config: config}, nil
}

// NewGRPCServer function
// to provide your users a way to instantiate your service, create a gRPC server,
// and register your service to that server
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// We specify the logger’s name to differentiate the server logs from other logs
	// in our service. Then we add a “grpc.time_ns” field to our structured logs to log
	// the duration of each request in nanoseconds.
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns", duration.Nanoseconds())
			},
		),
	}

	// We’ve configured OpenCensus to always sample the traces because we’re
	// developing our service and we want all of our requests traced.
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	// The views specify what stats OpenCensus will collect. The default server views
	// track stats on:
	// • Received bytes per RPC
	// • Sent bytes per RPC
	// • Latency
	// • Completed RPCs
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	// 1. We hook up our authenticate() interceptor to our gRPC server so that our server
	// identifies the subject of each RPC to kick off the authorization process.
	// 2. We configure gRPC to apply the Zap interceptors that log the gRPC
	// calls and attach OpenCensus as the server’s stat handler so that OpenCensus
	// can record stats on the server’s request handling.
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// authenticate(context.Context) function is an interceptor that reads the subject
// out of the client’s cert and writes it to the RPC’s context. With interceptors,
// you can intercept and modify the execution of each RPC call, allowing you to
// break the request handling into smaller, reusable chunks
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "could not find peer info").Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

// The subject(context.Context) function returns
// the client’s cert’s subject so we can identify a client and check their access
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
