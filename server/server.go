package server

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"

	"github.com/nireo/dcache/pb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

// Cache interface that represents the most basic operations of the cache.
type Cache interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
}

// ServerFinder is to combat compatibility issues with adding GetServers() to the
// cache struct. Since that would make us change the tests fully since we cannot
// use bigcache easily.
type ServerFinder interface {
	GetServers() ([]*pb.Server, error)
}

type grpcImpl struct {
	pb.UnsafeCacheServer
	c  Cache
	sf ServerFinder
}

func newimpl(c Cache) *grpcImpl {
	return &grpcImpl{
		c: c,
	}
}

// NewServer returns a grpc.Server with the given options applied.
func NewServer(cache Cache, grpcOpts ...grpc.ServerOption) (
	*grpc.Server, error,
) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		)),
	)

	grsv := grpc.NewServer(grpcOpts...)
	srv := newimpl(cache)
	pb.RegisterCacheServer(grsv, srv)

	return grsv, nil
}

// NewServer returns a grpc.Server with the given options applied.
func NewServerWithGetter(cache Cache, getter ServerFinder, grpcOpts ...grpc.ServerOption) (
	*grpc.Server, error,
) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		)),
	)

	grsv := grpc.NewServer(grpcOpts...)
	srv := newimpl(cache)
	srv.sf = getter
	pb.RegisterCacheServer(grsv, srv)

	return grsv, nil
}

// Set handles Set requests by calling the internal Cache's Set function
func (s *grpcImpl) Set(ctx context.Context, req *pb.SetRequest) (
	*pb.Empty, error,
) {
	err := s.c.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// Get handles Get requests by calling the internal Cache's Get function.
func (s *grpcImpl) Get(ctx context.Context, req *pb.GetRequest) (
	*pb.GetResponse, error,
) {
	val, err := s.c.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: val}, nil
}

// GetServers returns addresses to all of the Raft servers.
func (s *grpcImpl) GetServers(ctx context.Context, req *pb.Empty) (
	*pb.GetServer, error,
) {
	servers, err := s.sf.GetServers()
	if err != nil {
		return nil, err
	}
	return &pb.GetServer{Server: servers}, nil
}
