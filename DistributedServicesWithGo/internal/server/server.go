package server

import (
	"context"
	"google.golang.org/grpc"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

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
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
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

func newgrpcServer(config *Config) (*grpcServer, error) {
	return &grpcServer{Config: config}, nil
}

// NewGRPCServer function
// to provide your users a way to instantiate your service, create a gRPC server,
// and register your service to that server
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
