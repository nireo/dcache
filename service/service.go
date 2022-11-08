package service

import (
	"fmt"
	"net"
	"sync"

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
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return nil, err
		}
	}

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
	// raftListener := s.mux.Match(func(reader io.Reader) bool {
	// 	b := make([]byte, 1)
	// 	if _, err := reader.Read(b); err != nil {
	// 		return false
	// 	}
	// 	return b[0] == 1
	// })

	return nil
}
