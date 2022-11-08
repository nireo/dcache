package service

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nireo/dcache/registry"
	"github.com/nireo/dcache/server"
	"github.com/nireo/dcache/store"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

// Service is what combines all of the things together.

type Config struct {
	DataDir        string   // where to store raft data.
	BindAddr       string   // serf addr.
	RPCPort        int      // port for raft and client connections
	StartJoinAddrs []string // addresses to join to
	Bootstrap      bool     // should bootstrap cluster?
	NodeName       string   // raft server id
}

// RPCAddr returns the host:RPCPort string
func (c *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Service struct {
	conf   Config
	mux    cmux.CMux
	server *grpc.Server
	store  *store.Store
	reg    *registry.Registry

	shutdown     bool
	shutdowns    chan struct{}
	shutdownlock sync.Mutex
}

func New(conf Config) (*Service, error) {
	s := &Service{
		conf:      conf,
		shutdowns: make(chan struct{}),
	}

	setupFns := []func() error{
		s.setupMux,
		s.setupStore,
		s.setupServer,
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

func (s *Service) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", s.conf.RPCPort)
	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	s.mux = cmux.New(l)
	return nil
}

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
	conf.LocalID = raft.ServerID(s.conf.NodeName)
	conf.Bootstrap = s.conf.Bootstrap

	var err error
	s.store, err = store.New(conf)
	if err != nil {
		return err
	}
	if s.conf.Bootstrap {
		_, err = s.store.WaitForLeader(3 * time.Second)
	}
	return err
}

func (s *Service) setupServer() error {
	var (
		opts []grpc.ServerOption
		err  error
	)

	s.server, err = server.NewServer(s.store, opts...)
	if err != nil {
		return err
	}

	grpcListener := s.mux.Match(cmux.Any())
	go func() {
		if err := s.server.Serve(grpcListener); err != nil {
			s.Close()
		}
	}()

	return err
}

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
			s.server.GracefulStop()
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

func (s *Service) serve() error {
	if err := s.mux.Serve(); err != nil {
		s.Close()
		return err
	}

	return nil
}

func (s *Service) setupRegistry() error {
	rpcAddr, err := s.conf.RPCAddr()
	if err != nil {
		return err
	}

	s.reg, err = registry.New(s.store, registry.Config{
		NodeName: s.conf.NodeName,
		BindAddr: s.conf.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: s.conf.StartJoinAddrs,
	})

	return err
}
