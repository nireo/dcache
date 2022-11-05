package store

import (
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type Store struct {
	raft    *raft.Raft
	raftDir string
	logger  *zap.Logger
}

// New creates a store instance.
func New() (*Store, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	return &Store{
		raft:    nil,
		raftDir: "",
		logger:  logger,
	}, nil
}

// Close down this raft node and flush out possible data in the logger.
func (s *Store) Close() error {
	s.logger.Sync()

	f := s.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	return nil
}

func (s *Store) isLeader() bool {
	return s.raft.State() == raft.Leader
}

// remove removes a node from the raft cluster.
func (s *Store) remove(id string) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil {
		if f.Error() == raft.ErrNotLeader {
			return raft.ErrNotLeader
		}
		return f.Error()
	}

	return nil
}

func (s *Store) Join(id, addr string) error {
	s.logger.Info("join request", zap.String("id", id), zap.String("addr", addr))

	// only leader can make modifications to the cluster.
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	srvID := raft.ServerID(id)
	srvAddr := raft.ServerAddress(addr)

	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		s.logger.Error("failed to get configuration", zap.Error(err))
		return err
	}

	for _, srv := range f.Configuration().Servers {
		if srv.ID == srvID || srv.Address == srvAddr {
			// already exists in the cluster.
			if srv.ID == srvID && srv.Address == srvAddr {
				return nil
			}

			if err := s.remove(id); err != nil {
				s.logger.Error("failed to remove node", zap.Error(err))
				return err
			}

			s.logger.Info("removed node to rejoin with changed info", zap.String("id", id))
		}
	}

	addFuture := s.raft.AddVoter(srvID, srvAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		s.logger.Error("failed adding voter", zap.Error(err))
		return err
	}

	s.logger.Info("node joined successfully", zap.String("id", id), zap.String("addr", addr))
	return nil
}

func (s *Store) Leave(id string) error {
	s.logger.Info("leave request for node", zap.String("id", id))
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	if err := s.remove(id); err != nil {
		s.logger.Error("failed removing node", zap.Error(err))
		return err
	}

	s.logger.Info("node removed", zap.String("id", id))
	return nil
}

func (s *Store) LeaderAddr() string {
	return string(s.raft.Leader())
}
