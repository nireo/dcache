package store

import (
	"bytes"
	"testing"
)

func xd(d []byte) {
	return
}

func xd1(a1 byte, a2 string, a3 []byte) {
	return
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

	if flag != SetOperation {
		t.Fatalf("flag was incorrect. got=%d want=%d", flag, SetOperation)
	}

	if key2 != key {
		t.Fatalf("key was incorrect. got=%s want=%s", key2, key)
	}

	if !bytes.Equal(val2, val) {
		t.Fatalf("val was incorrect. got=%s want=%s", string(val2), string(val))
	}
}
