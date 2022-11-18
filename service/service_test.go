package service_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/nireo/dcache/api"
	"github.com/nireo/dcache/service"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	rpcaddr, err := serv.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcaddr, opts...)
	require.NoError(t, err)

	return api.NewCacheClient(conn)
}

func httpGetHelper(t *testing.T, addr string) []byte {
	t.Helper()

	resp, err := http.Get(addr)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return body
}

type setupConf struct {
	enablehttp bool
	enablegrpc bool
}

func setupNServices(t *testing.T, n int, conf setupConf) []*service.Service {
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
			EnableGRPC:     conf.enablegrpc,
			EnableHTTP:     conf.enablehttp,
		})
		require.NoError(t, err)

		services = append(services, service)
	}

	t.Cleanup(func() {
		for _, s := range services {
			s.Close()
			require.NoError(t, os.RemoveAll(s.Config.DataDir))
		}
	})

	return services
}

func TestGRPC(t *testing.T) {
	services := setupNServices(t, 3, setupConf{
		enablehttp: false,
		enablegrpc: true,
	})
	// give some time for the cluster to setup
	time.Sleep(2 * time.Second)

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

func TestHTTP(t *testing.T) {
	services := setupNServices(t, 3, setupConf{
		enablehttp: true,
		enablegrpc: false,
	})
	time.Sleep(3 * time.Second)

	leaderAddr, err := services[0].Config.RPCAddr()
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/testkey", leaderAddr),
		"text/plain",
		bytes.NewBuffer([]byte("testval")),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := httpGetHelper(t, fmt.Sprintf("http://%s/testkey", leaderAddr))
	require.Equal(t, []byte("testval"), body)

	time.Sleep(1 * time.Second)

	followerAddr, err := services[1].Config.RPCAddr()
	require.NoError(t, err)
	body = httpGetHelper(t, fmt.Sprintf("http://%s/testkey", followerAddr))
	require.Equal(t, []byte("testval"), body)
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

func TestBothCommunication(t *testing.T) {
	services := setupNServices(t, 3, setupConf{
		enablehttp: true,
		enablegrpc: true,
	})
	// give some time for the cluster to setup
	time.Sleep(2 * time.Second)

	leaderAddr, err := services[0].Config.RPCAddr()
	require.NoError(t, err)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/testkey", leaderAddr),
		"text/plain",
		bytes.NewBuffer([]byte("testval")),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := httpGetHelper(t, fmt.Sprintf("http://%s/testkey", leaderAddr))
	require.Equal(t, []byte("testval"), body)

	time.Sleep(1 * time.Second)

	followerClient := createClient(t, services[1])
	r, err := followerClient.Get(context.Background(), &api.GetRequest{
		Key: "testkey",
	})
	require.NoError(t, err)
	require.Equal(t, []byte("testval"), r.Value)
}
