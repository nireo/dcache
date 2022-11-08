package server

import (
	"context"

	"github.com/nireo/dcache/api"
	"google.golang.org/grpc"
)

type Cache interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
}

type grpcImpl struct {
	api.UnsafeCacheServer
	c Cache
}

func newimpl(c Cache) *grpcImpl {
	return &grpcImpl{
		c: c,
	}
}

func NewServer(cache Cache, grpcOpts ...grpc.ServerOption) (
	*grpc.Server, error,
) {
	grsv := grpc.NewServer(grpcOpts...)
	srv := newimpl(cache)
	api.RegisterCacheServer(grsv, srv)

	return grsv, nil
}

func (s *grpcImpl) Set(ctx context.Context, req *api.SetRequest) (
	*api.Empty, error,
) {
	err := s.c.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &api.Empty{}, nil
}

func (s *grpcImpl) Get(ctx context.Context, req *api.GetRequest) (
	*api.GetResponse, error,
) {
	val, err := s.c.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &api.GetResponse{Value: val}, nil
}
