package store

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

const (
	SetOperation byte = iota
	GetOperation
)

// don't need a complicated serializer/deserializer since our data format is
// quite simple.
func serializeEntry(flag byte, key string, val []byte) []byte {
	// HEADER: (FLAG 1byte) + (KEY_SIZE uint32 4bytes) (KEY_DATA) +
	// (VALUE_SIZE uint32 4bytes) + (VALUE_DATA)

	buf := make([]byte, 1+4+len(key)+4+len(val))
	buf[0] = flag
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(key)))
	copy(buf[5:], []byte(key))
	binary.LittleEndian.PutUint32(buf[5+len(key):], uint32(len(val)))
	copy(buf[5+len(key)+4:], val)

	return buf
}

func deserializeEntry(buf []byte) (byte, string, []byte) {
	keySize := binary.LittleEndian.Uint32(buf[1:])
	key := string(buf[5 : 5+keySize])
	return buf[0], key,
		buf[(5 + keySize + 4) : binary.LittleEndian.Uint32(buf[5+keySize:])+(5+keySize+4)]
}

type Store struct {
	raft    *raft.Raft
	raftDir string
	logger  *zap.Logger

	cache *bigcache.BigCache
}

type applyResult struct {
	res any
	err error
}

// New creates a store instance.
func New(dataDir string) (*Store, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	// setup a cache
	cache, err := bigcache.New(context.Background(), bigcache.DefaultConfig(10*time.Minute))
	if err != nil {
		return nil, err
	}

	return &Store{
		raft:    nil,
		raftDir: filepath.Join(dataDir, "raft"),
		logger:  logger,
		cache:   cache,
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

func (s *Store) Set(key string, value []byte) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	return nil
}
