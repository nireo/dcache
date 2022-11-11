package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/nireo/dcache/api"
	"github.com/nireo/dcache/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupTest(t *testing.T, fn func(server.Cache)) (
	client api.CacheClient, cleanup func(),
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

	client = api.NewCacheClient(cc)
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

	_, err := client.Set(ctx, &api.SetRequest{
		Key:   "testkey",
		Value: []byte("testvalue"),
	})
	require.NoError(t, err)
	res, err := client.Get(ctx, &api.GetRequest{
		Key: "testkey",
	})
	require.NoError(t, err)

	require.Equal(t, []byte("testvalue"), res.Value)
}
