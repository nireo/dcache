package store

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/hashicorp/raft"
	"github.com/nireo/dcache/pb"
	fastlog "github.com/tidwall/raft-fastlog"
	"go.uber.org/zap"
)

const (
	// SetOperation is for handling set operations in raft_apply.
	SetOperation byte = iota

	// GetOperation is for handling get operations in raft_apply.
	GetOperation
)

// ErrJoiningSelf represents the situation where a node tries to join itself.
var ErrJoiningSelf = errors.New("trying to join self")

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

// deserializeEntry takes in the bytes that serializeEntry created and parses the
// entrie's details.
func deserializeEntry(buf []byte) (byte, string, []byte) {
	keySize := binary.LittleEndian.Uint32(buf[1:])
	key := string(buf[5 : 5+keySize])
	return buf[0], key,
		buf[(5 + keySize + 4) : binary.LittleEndian.Uint32(buf[5+keySize:])+(5+keySize+4)]
}

// Store represents a Raft node. It also implements the FSM interface that
// raft provides. So we can directly modify the cache stored in this struct.
type Store struct {
	conf    Config
	raft    *raft.Raft
	raftDir string
	logger  *zap.Logger

	cache *bigcache.BigCache
}

// Config represents all of the user configurable fields for the Raft node.
// Note that without setting these defaults are also fine. Only Transport and
// LocalID are important.
type Config struct {
	DataDir           string
	BindAddr          string
	Bootstrap         bool
	LocalID           raft.ServerID
	SnapshotThreshold uint64
	StrongConsistency bool

	// Timeouts
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	LeaderLeaseTimeout time.Duration

	Transport *Transport
}

// snapshot represents a snapshot of the finite state machine. It copies the address
// to the cache stored in the Raft node and copies all of the entries into the io.Writer
// that raft provides.
type snapshot struct {
	start time.Time
	cache *bigcache.BigCache
}

// applyResult represents a generic result from raft_apply. We need the error field here
// because error in this struct means error in our code, and error returned from raft means
// errors in either communication or some other raft internals.
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
		conf:   conf,
	}

	transport := raft.NewNetworkTransport(conf.Transport, 5, 10*time.Second, os.Stderr)
	stableStore, err := fastlog.NewFastLogStore(":memory:", fastlog.Medium, io.Discard)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.SnapshotThreshold = conf.SnapshotThreshold
	config.LocalID = conf.LocalID

	if conf.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = conf.HeartbeatTimeout
	}

	if conf.ElectionTimeout != 0 {
		config.ElectionTimeout = conf.ElectionTimeout
	}

	if conf.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = conf.LeaderLeaseTimeout
	}

	if conf.CommitTimeout != 0 {
		config.CommitTimeout = conf.CommitTimeout
	}

	store.raft, err = raft.NewRaft(
		config,
		store,
		stableStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, err
	}

	if conf.Bootstrap {
		conf := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(conf.Transport.Addr().String()),
			}},
		}
		err = store.raft.BootstrapCluster(conf).Error()
	}
	return store, err
}

// Close down this raft node and flush out possible data in the logger.
func (s *Store) Close() error {
	s.logger.Sync()

	// close raft
	f := s.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	// close internal cache
	return s.cache.Close()
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

// joinHelper makes adding nodes to the raft closer easier. We need a helper since
// the voter field is optional and this function correctly handles that field. This
// function is called by store.Join() and store.JoinNonVoter()
func (s *Store) joinHelper(id, addr string, voter bool) error {
	s.logger.Info("join request", zap.String("id", id), zap.String("addr", addr))

	// only leader can make modifications to the cluster.
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	srvID := raft.ServerID(id)
	srvAddr := raft.ServerAddress(addr)

	if srvID == s.conf.LocalID {
		return ErrJoiningSelf
	}

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

	var addFuture raft.IndexFuture
	if voter {
		addFuture = s.raft.AddVoter(srvID, srvAddr, 0, 0)
	} else {
		addFuture = s.raft.AddNonvoter(srvID, srvAddr, 0, 0)
	}

	if f := addFuture.(raft.Future); f.Error() != nil {
		return f.Error()
	}

	s.logger.Info("node joined successfully", zap.String("id", id), zap.String("addr", addr))
	return nil
}

// JoinNonVoter adds a node that cannot write into the cluster. These are useful for
// adding more reability to reads.
func (s *Store) JoinNonVoter(id, addr string) error {
	return s.joinHelper(id, addr, false)
}

// Join adds a node with 'id' and 'addr' into the raft cluster. The address is the
// raft bind address of the node.
func (s *Store) Join(id, addr string) error {
	return s.joinHelper(id, addr, true)
}

// Leave removes a node from the cluster with the given id. This function is called
// by registry to let the raft node know that a node has left the cluster and that
// raft should not try to contact it anymore.
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
	r := res.(applyResult)
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

	if s.conf.StrongConsistency {
		if !s.isLeader() {
			return nil, raft.ErrNotLeader
		}

		res, err := s.createApplyReq(GetOperation, key, []byte{})
		if err != nil {
			return nil, err
		}

		r := res.(applyResult)
		return r.res.([]byte), r.err
	}

	return s.cache.Get(key)
}

// Snapshot takes a snapshot of the current finite state machine and logs the time
// so we can see how long a snapshot process took.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	ti := time.Now()
	s.logger.Info("started snapshot", zap.Time("start_time", ti))
	return &snapshot{
		start: ti,
		cache: s.cache,
	}, nil
}

// Restore takes in the bytes generated by snapshot.Persist() and parses the cache
// state from that.
func (s *Store) Restore(rc io.ReadCloser) error {
	// TODO
	return nil
}

// Persist writes the cache state into bytes and writes it into raft.SnapshotSink.
// The data is later parsed by Restore to create fill the finite state machine.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		iter := s.cache.Iterator()

		for iter.SetNext() {
			curr, err := iter.Value()
			if err != nil {
				return err
			}

			if _, err = sink.Write(serializeEntry(SetOperation, curr.Key(), curr.Value())); err != nil {
				return err
			}
		}

		return nil
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (s *snapshot) Release() {}

// WaitForLeader waits until a leader is elected. If a leader hasn't been elected in the
// given timeout return an error.
func (s *Store) WaitForLeader(t time.Duration) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(t)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l := s.LeaderAddr()
			if l != "" {
				return l, nil
			}
		case <-timer.C:
			return "", errors.New("wait for leader timeout expired")
		}
	}
}

// GetServers returns the server currently present in the node.
func (s *Store) GetServers() ([]*pb.Server, error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}

	ss := f.Configuration().Servers
	srvs := make([]*pb.Server, len(ss))
	for i := range ss {
		srvs[i] = &pb.Server{
			Id:         string(ss[i].ID),
			RpcAddr:    string(ss[i].Address),
			IsLeader:   s.raft.Leader() == ss[i].Address,
			VoteStatus: ss[i].Suffrage.String(),
		}
	}

	return srvs, nil
}

// Stepdown forces the cluster to change the leadership.
func (s *Store) Stepdown(wait bool) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	f := s.raft.LeadershipTransfer()
	if !wait {
		return nil
	}

	return f.Error()
}
