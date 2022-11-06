package store

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/hashicorp/raft"
	fastlog "github.com/tidwall/raft-fastlog"
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

type Config struct {
	DataDir   string
	BindAddr  string
	Bootstrap bool
}

type applyResult struct {
	res any
	err error
}

// New creates a store instance.
func New(conf Config) (*Store, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	raftDir := filepath.Join(conf.DataDir, "raft")

	// setup a cache
	cache, err := bigcache.New(context.Background(), bigcache.DefaultConfig(10*time.Minute))
	if err != nil {
		return nil, err
	}

	store := &Store{
		raft:   nil,
		logger: logger,
		cache:  cache,
	}

	addr, err := net.ResolveTCPAddr("tcp", conf.BindAddr)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(conf.BindAddr, addr, 10, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	stableStore, err := fastlog.NewFastLogStore(":memory:", fastlog.Medium, io.Discard)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.SnapshotThreshold = 16384

	store.raft, err = raft.NewRaft(config, store, stableStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	if conf.Bootstrap {
		conf := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(conf.BindAddr),
			}},
		}
		err = store.raft.BootstrapCluster(conf).Error()
	}
	return store, err
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

// isLeader returns a boolean based on if the node is a leader or not.
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

// Join adds a node with 'id' and 'addr' into the raft cluster. The address is the
// raft bind address of the node.
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

// Leave removes a node from the cluster with the given id.
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

// LeaderAddr returns the raft bind address of the leader node.
func (s *Store) LeaderAddr() string {
	return string(s.raft.Leader())
}

// Apply handles the applyRequest made by the createApplyReq function. It returns a
// applyResult struct such that handler functions can properly handle the given error.
func (s *Store) Apply(l *raft.Log) interface{} {
	flag, key, value := deserializeEntry(l.Data)

	switch flag {
	case SetOperation:
		return applyResult{res: nil, err: s.cache.Set(key, value)}
	case GetOperation:
		val, err := s.cache.Get(key)
		return applyResult{res: val, err: err}
	}
	return nil
}

// Set applies a given key-value pair into the raft cluster. Since writing a key
// is a leader-only operation, we need to check for that as well.
func (s *Store) Set(key string, value []byte) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	res, err := s.createApplyReq(SetOperation, key, value)
	if err != nil {
		// error in raft processing
		return err
	}

	// error writing to cache on leader.
	r := res.(*applyResult)
	return r.err
}

// createApplyReq sends formulates data in a good way and sends the request with the data
// to raft.Apply(), which is in turn handled by our Apply() function on another raft node.
func (s *Store) createApplyReq(ty byte, key string, value []byte) (interface{}, error) {
	buffer := serializeEntry(ty, key, value)

	f := s.raft.Apply(buffer, 10*time.Second)
	if err := f.Error(); err != nil {
		return nil, err
	}

	r := f.Response()
	if err, ok := r.(error); ok {
		return nil, err
	}
	return r, nil
}

// Get finds a value with a given key either from this node's cache, or the leader.
// If the value is retrieved from a non-leader node, we risk the chance of the value
// not existing, or being old. On the other hand, request the value from the leader
// adds a lot of overhead.
func (s *Store) Get(key string) ([]byte, error) {
	// TODO: strong consistency aka get from leader

	return s.cache.Get(key)
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (s *Store) Restore(rc io.ReadCloser) error {
	return nil
}
