package service_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nireo/dcache/api"
	"github.com/nireo/dcache/service"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func genNPorts(n int) []int {
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i], _ = getFreePort()
	}
	return ports
}

func createClient(t *testing.T, serv *service.Service) api.CacheClient {
	opts := []grpc.DialOption{grpc.WithInsecure()}
	rpcaddr, err := serv.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcaddr, opts...)
	require.NoError(t, err)

	return api.NewCacheClient(conn)
}

func TestServ(t *testing.T) {
	var services []*service.Service

	for i := 0; i < 3; i++ {
		ports := genNPorts(2)
		bindaddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		datadir, err := os.MkdirTemp("", "service-test")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, services[0].Config.BindAddr)
		}

		service, err := service.New(service.Config{
			NodeName:       fmt.Sprintf("%d", i),
			Bootstrap:      i == 0,
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindaddr,
			DataDir:        datadir,
			RPCPort:        rpcPort,
			EnableGRPC:     true,
			EnableHTTP:     false,
		})
		require.NoError(t, err)

		services = append(services, service)
	}

	defer func() {
		// cleanup
		for _, s := range services {
			s.Close()
			require.NoError(t, os.RemoveAll(s.Config.DataDir))
		}
	}()

	// give some time for the cluster to setup
	time.Sleep(3 * time.Second)

	leaderClient := createClient(t, services[0])
	_, err := leaderClient.Set(context.Background(), &api.SetRequest{
		Key:   "key1",
		Value: []byte("value1"),
	})
	require.NoError(t, err)

	r, err := leaderClient.Get(context.Background(), &api.GetRequest{
		Key: "key1",
	})
	require.NoError(t, err)

	require.Equal(t, []byte("value1"), r.Value)

	time.Sleep(3 * time.Second)

	followerClient := createClient(t, services[1])
	r, err = followerClient.Get(context.Background(), &api.GetRequest{
		Key: "key1",
	})
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), r.Value)
}

func TestNoCommunication(t *testing.T) {
	_, err := service.New(service.Config{
		NodeName:       "node",
		Bootstrap:      true,
		StartJoinAddrs: nil,
		BindAddr:       "localhost:8080",
		DataDir:        "./data",
		RPCPort:        9200,
		EnableGRPC:     false,
	})
	require.Error(t, err)
	require.Equal(t, service.ErrNoCommunication, err)
}
