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

func genNPorts(n int) []int {
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i], _ = getFreePort()
	}
	return ports
}

func newTestStore(t *testing.T, port, id int, bootstrap bool) (*Store, error) {
	datadir, err := os.MkdirTemp("", "store-test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(datadir)
	})

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

	return New(conf)
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
