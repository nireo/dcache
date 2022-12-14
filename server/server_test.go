package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/nireo/dcache/pb"
	"github.com/nireo/dcache/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type mockCache struct{}

func (m *mockCache) Get(key string) ([]byte, error) {
	return nil, nil
}

func (m *mockCache) Set(key string, val []byte) error {
	return nil
}

func setupTest(t *testing.T, fn func(server.Cache)) (
	client pb.CacheClient, cleanup func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	testCache, err := bigcache.New(context.Background(), bigcache.DefaultConfig(10*time.Minute))
	require.NoError(t, err)

	srv, err := server.NewServer(testCache)
	require.NoError(t, err)

	go func() {
		srv.Serve(l)
	}()

	client = pb.NewCacheClient(cc)
	return client, func() {
		srv.Stop()
		cc.Close()
		l.Close()
		testCache.Close()
	}
}

func TestSetGet(t *testing.T) {
	client, cleanup := setupTest(t, nil)
	defer cleanup()

	ctx := context.Background()

	_, err := client.Set(ctx, &pb.SetRequest{
		Key:   "testkey",
		Value: []byte("testvalue"),
	})
	require.NoError(t, err)
	res, err := client.Get(ctx, &pb.GetRequest{
		Key: "testkey",
	})
	require.NoError(t, err)

	require.Equal(t, []byte("testvalue"), res.Value)
}

// testConnection
type subConn struct {
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}

func setupPickerTest() (*server.Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}

	picker := &server.Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &server.Picker{}
	for _, method := range []string{
		"/cache.v1.Cache/Set",
		"/cache.v1.Cache/Get",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}

		res, err := picker.Pick(info)
		require.Nil(t, res.SubConn)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
	}
}

func TestPickerChoosesLeader(t *testing.T) {
	picker, subConns := setupPickerTest()
	info := balancer.PickInfo{
		FullMethodName: "/cache.v1.Cache/Set",
	}

	// try multiple times to ensure that leader is always selected.
	for i := 0; i < 10; i++ {
		pick, err := picker.Pick(info)

		require.NoError(t, err)
		require.Equal(t, subConns[0], pick.SubConn)
	}
}

func TestPickerUsesFollowers(t *testing.T) {
	picker, subConns := setupPickerTest()
	info := balancer.PickInfo{
		FullMethodName: "/cache.v1.Cache/Get",
	}

	// try multiple times to ensure that leader is always selected.
	for i := 0; i < 10; i++ {
		pick, err := picker.Pick(info)

		require.NoError(t, err)
		require.Equal(t, subConns[i%2+1], pick.SubConn)
	}
}

type getServers struct{}

func (s *getServers) GetServers() ([]*pb.Server, error) {
	return []*pb.Server{{
		Id:       "leader",
		RpcAddr:  "localhost:9001",
		IsLeader: true,
	}, {
		Id:      "follower",
		RpcAddr: "localhost:9002",
	}}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(
	config string,
) *serviceconfig.ParseResult {
	return nil
}

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := server.NewServerWithGetter(&mockCache{}, &getServers{})
	require.NoError(t, err)
	go srv.Serve(l)

	conn := &clientConn{}
	r := &server.Resolver{}

	_, err = r.Build(resolver.Target{
		Endpoint: l.Addr().String(),
	}, conn, resolver.BuildOptions{})
	require.NoError(t, err)

	wantState := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	require.Equal(t, wantState, conn.state)
	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}
