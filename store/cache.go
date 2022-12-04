package store

import (
	"context"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/allegro/bigcache/v3"
)

type Cache interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
}

// NewBigcache creates a big cache interface that implements the Cache interface.
// Bigcache is slower but can store entries that are larger. While fastcache is suitable
// for smaller entries and at the same time a bit faster.
func NewBigcache() (*bigcache.BigCache, error) {
	cache, err := bigcache.New(context.Background(), bigcache.DefaultConfig(10*time.Minute))
	if err != nil {
		return nil, err
	}

	return cache, nil
}

func NewFastcache(maxbytes int) *fastcache.Cache {
	return fastcache.New(maxbytes)
}
