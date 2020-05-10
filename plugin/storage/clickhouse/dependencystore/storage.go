package dependencystore

import (
	"errors"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	errNotImplemented = errors.New("not implemented")
)

type Reader interface {
	GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error)
}

// DependencyStore handles all queries and insertions to Clickhouse dependencies
type DependencyStore struct {
	reader spanstore.Reader
}

// NewDependencyStore returns a DependencyStore
func NewDependencyStore(store spanstore.Reader) *DependencyStore {
	return &DependencyStore{
		reader: store,
	}
}

// GetDependencies returns all interservice dependencies, implements DependencyReader
func (s *DependencyStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, errNotImplemented
}
