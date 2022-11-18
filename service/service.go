package service

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	httpd "github.com/nireo/dcache/http"
	"github.com/nireo/dcache/registry"
	"github.com/nireo/dcache/server"
	"github.com/nireo/dcache/store"
	"github.com/soheilhy/cmux"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
)

var ErrNoCommunication = errors.New("no communication pathways for clients")

// Config handles all of the customizable values for Service.
type Config struct {
	DataDir        string   // where to store raft data.
	BindAddr       string   // serf addr.
	RPCPort        int      // port for raft and client connections
	StartJoinAddrs []string // addresses to join to
	Bootstrap      bool     // should bootstrap cluster?
	NodeName       string   // raft server id

	// Enable different communications protocols for clients
	EnableHTTP bool
	EnableGRPC bool
}

// RPCAddr returns the host:RPCPort string
func (c *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// HTTPAddr returns the HTTP address to the server.
func (c *Config) HTTPAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%d", host, c.RPCPort), nil
}

// Service handles combining every component of the system.
type Service struct {
	Config Config
	mux    cmux.CMux
	server *grpc.Server
	store  *store.Store
	reg    *registry.Registry

	httpListener net.Listener
	grpcListener net.Listener

	shutdown     bool
	shutdowns    chan struct{}
	shutdownlock sync.Mutex
}

// New returns a new service instance. This function also sets up the
// registry, store, mux and server fields.
func New(conf Config) (*Service, error) {
	s := &Service{
		Config:    conf,
		shutdowns: make(chan struct{}),
	}

	// check that either HTTP or gRPC is enabled. Otherwise user cannot really
	// interact with the cluster.
	if !s.Config.EnableGRPC && !s.Config.EnableHTTP {
		return nil, ErrNoCommunication
	}

	if err := s.setupMux(); err != nil {
		return nil, err
	}

	// We need to setup stores in a different order since the order the connections
	// are matched in matters and we need the store instance to setup servers.
	if s.Config.EnableGRPC {
		s.grpcListener = s.mux.MatchWithWriters(
			cmux.HTTP2MatchHeaderFieldPrefixSendSettings("content-type", "application/grpc"),
		)
	}

	if s.Config.EnableHTTP {
		s.httpListener = s.mux.Match(cmux.HTTP1Fast())
	}

	setupFns := []func() error{
		s.setupStore,
		s.setupServer,
		s.setupHTTP,
		s.setupRegistry,
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go s.serve()

	return s, nil
}

// setupMux sets up the connection multiplexer.
func (s *Service) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", s.Config.RPCPort)
	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	s.mux = cmux.New(l)
	return nil
}

// setupStore sets up the raft store.
func (s *Service) setupStore() error {
	raftListener := s.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return b[0] == 1
	})

	conf := store.Config{}
	conf.Transport = store.NewTransport(raftListener)
	conf.LocalID = raft.ServerID(s.Config.NodeName)
	conf.Bootstrap = s.Config.Bootstrap

	var err error
	s.store, err = store.New(conf)
	if err != nil {
		return err
	}
	if s.Config.Bootstrap {
		_, err = s.store.WaitForLeader(3 * time.Second)
	}
	return err
}

// setupServer sets up the grpc server. The grpc server is for clients to interact
// with the service.
func (s *Service) setupServer() error {
	if s.grpcListener == nil {
		return nil
	}

	var (
		opts []grpc.ServerOption
		err  error
	)

	s.server, err = server.NewServer(s.store, opts...)
	if err != nil {
		return err
	}

	go func() {
		if err := s.server.Serve(s.grpcListener); err != nil {
			s.Close()
		}
	}()
	return nil
}

// Close shuts dwon components and leaves the registry cluster.
func (s *Service) Close() error {
	s.shutdownlock.Lock()
	defer s.shutdownlock.Unlock()

	if s.shutdown {
		return nil
	}
	s.shutdown = true
	close(s.shutdowns)

	closeFns := []func() error{
		s.reg.Leave,
		func() error {
			if s.Config.EnableGRPC {
				s.server.GracefulStop()
			}
			return nil
		},
		s.store.Close,
	}

	for _, fn := range closeFns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

// serve runs the connection multiplexer to start serving connections.
func (s *Service) serve() error {
	if err := s.mux.Serve(); err != nil {
		s.Close()
		return err
	}
	return nil
}

// setupRegistry sets up the service discovery module.
func (s *Service) setupRegistry() error {
	rpcAddr, err := s.Config.RPCAddr()
	if err != nil {
		return err
	}

	s.reg, err = registry.New(s.store, registry.Config{
		NodeName: s.Config.NodeName,
		BindAddr: s.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: s.Config.StartJoinAddrs,
	})

	return err
}

// setupHTTP sets up a HTTP handler to interact with the store.
func (s *Service) setupHTTP() error {
	if s.httpListener == nil {
		// http not enabled
		return nil
	}

	httpServer, err := httpd.New(s.store)
	if err != nil {
		return err
	}

	go fasthttp.Serve(s.httpListener, httpServer.Handler)

	return nil
}
