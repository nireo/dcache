package store

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func xd(d []byte) {
}

func xd1(a1 byte, a2 string, a3 []byte) {
}

func BenchmarkSerialize(b *testing.B) {
	b.ReportAllocs()
	val := []byte("this is some very complex analytical data")
	for i := 0; i < b.N; i++ {
		data := serializeEntry(0, "test/entry/very/complicated/yes", val)
		xd(data)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	b.ReportAllocs()
	val := []byte("this is some very complex analytical data")
	data := serializeEntry(0, "test/entry/very/complicated/yes", val)
	for i := 0; i < b.N; i++ {
		xd1(deserializeEntry(data))
	}
}

func TestSerialization(t *testing.T) {
	val := []byte("this is some very complex analytical data")
	key := "test/entry/very/complicated/yes"
	data := serializeEntry(SetOperation, key, val)

	flag, key2, val2 := deserializeEntry(data)

	require.Equal(t, SetOperation, flag, "flag was not equal to set operation")
	require.Equal(t, key, key2, "decoded key is not the same as the original")
	require.Equal(t, val, val2, "decoded value is no the same as the original")
}

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

func newTestStore(t *testing.T, port, id int, bootstrap bool) (*Store, error) {
	datadir, err := os.MkdirTemp("", "store-test")
	require.NoError(t, err)

	conf := Config{}
	conf.BindAddr = fmt.Sprintf("localhost:%d", port)
	conf.LocalID = raft.ServerID(fmt.Sprintf("%d", id))
	conf.Bootstrap = bootstrap
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.SnapshotThreshold = 10000
	conf.DataDir = datadir

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	conf.Transport = &Transport{
		ln: ln,
	}

	s, err := New(conf)
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		os.RemoveAll(datadir)
		s.Close()
	})

	return s, nil
}

func TestSingleNode(t *testing.T) {
	port, _ := getFreePort()

	store, err := newTestStore(t, port, 1, true)
	require.NoError(t, err)

	store.WaitForLeader(3 * time.Second)

	err = store.Set("entry1", []byte("garbage"))
	require.NoError(t, err)

	val, err := store.Get("entry1")
	require.NoError(t, err)

	require.Equal(t, []byte("garbage"), val)
}

func TestJoinSelf(t *testing.T) {
	port, _ := getFreePort()

	store, err := newTestStore(t, port, 1, true)
	require.NoError(t, err)

	store.WaitForLeader(3 * time.Second)

	err = store.Join("1", "localhost:1234")
	require.Equal(t, ErrJoiningSelf, err)
}

func TestMultipleNodes(t *testing.T) {
	nodeCount := 3
	var err error
	stores := make([]*Store, nodeCount)

	for i := 0; i < nodeCount; i++ {
		port, _ := getFreePort()
		stores[i], err = newTestStore(t, port, i, i == 0)
		require.NoError(t, err)

		if i != 0 {
			fmt.Println("started joining")
			err = stores[0].Join(
				string(stores[i].conf.LocalID),
				string(stores[i].conf.Transport.Addr().String()),
			)
			require.NoError(t, err)
		} else {
			_, err = stores[i].WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}
	}

	type testPair struct {
		key   string
		value []byte
	}

	pairs := []testPair{
		{
			key:   "hello1",
			value: []byte("value1"),
		},
		{
			key:   "hello2",
			value: []byte("value2"),
		},
	}

	stores[0].WaitForLeader(3 * time.Second)

	for _, pr := range pairs {
		err := stores[0].Set(pr.key, pr.value)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				val, err := stores[j].Get(pr.key)
				if err != nil {
					return false
				}
				require.Equal(t, val, pr.value)
			}

			return true
		}, 5*time.Second, 100*time.Millisecond)
	}

	servers, err := stores[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	err = stores[0].Leave("1")
	require.NoError(t, err)

	err = stores[0].Set("hello3", []byte("value3"))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	val, err := stores[1].Get("hello3")
	require.Nil(t, val)
	require.Error(t, err)

	val, err = stores[2].Get("hello3")
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), val)
}
