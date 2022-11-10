package server

import (
	"context"

	"github.com/nireo/dcache/api"
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
	GetServers() ([]*api.Server, error)
}

type grpcImpl struct {
	api.UnsafeCacheServer
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
	grsv := grpc.NewServer(grpcOpts...)
	srv := newimpl(cache)
	api.RegisterCacheServer(grsv, srv)

	return grsv, nil
}

// Set handles Set requests by calling the internal Cache's Set function
func (s *grpcImpl) Set(ctx context.Context, req *api.SetRequest) (
	*api.Empty, error,
) {
	err := s.c.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &api.Empty{}, nil
}

// Get handles Get requests by calling the internal Cache's Get function.
func (s *grpcImpl) Get(ctx context.Context, req *api.GetRequest) (
	*api.GetResponse, error,
) {
	val, err := s.c.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &api.GetResponse{Value: val}, nil
}

// GetServers returns addresses to all of the Raft servers.
func (s *grpcImpl) GetServers(ctx context.Context, req *api.Empty) (
	*api.GetServer, error,
) {
	servers, err := s.sf.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServer{Server: servers}, nil
}
