package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/nireo/dcache/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const ResolverName string = "dcache"

type Resolver struct {
	sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	log           *zap.Logger
}

func init() {
	resolver.Register(&Resolver{})
}

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.log = zap.L().Named("resolver")
	r.clientConn = cc

	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, ResolverName),
	)

	var err error
	dialopts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialopts...)
	if err != nil {
		return nil, err
	}

	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return ResolverName
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.Lock()
	defer r.Unlock()
	client := pb.NewCacheClient(r.resolverConn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &pb.Empty{})
	if err != nil {
		r.log.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}

	addrs := make([]resolver.Address, len(res.Server))
	for i := range res.Server {
		addrs[i] = resolver.Address{
			Addr: res.Server[i].RpcAddr,
			Attributes: attributes.New(
				"is_leader", res.Server[i].IsLeader,
			),
		}
	}

	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close tears down clientConn and all underlying connections
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.log.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
