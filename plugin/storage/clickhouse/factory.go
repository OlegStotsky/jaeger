package clickhouse

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	dependencyStore "github.com/jaegertracing/jaeger/plugin/storage/clickhouse/dependencystore"
	store "github.com/jaegertracing/jaeger/plugin/storage/clickhouse/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	primaryNamespace = "clickhouse"
	archiveNamespace = "clickhouse-archive"
)

// Factory implements storage.Factory for Clickhouse backend.
type Factory struct {
	logger  *zap.Logger
	Options *Options
	db      *sql.DB
	archive *sql.DB

	makeReader readerMaker
	makeWriter writerMaker
}

type readerMaker func(db *sql.DB, operationsTable, indexTable, spansTable string) (spanstore.Reader, error)
type writerMaker func(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding store.Encoding, delay time.Duration, size int) (spanstore.Writer, error)

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{
		Options: NewOptions(primaryNamespace, archiveNamespace),

		makeReader: func(db *sql.DB, operationsTable, indexTable, spansTable string) (spanstore.Reader, error) {
			return store.NewTraceReader(db, operationsTable, indexTable, spansTable), nil
		},
		makeWriter: func(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding store.Encoding, delay time.Duration, size int) (spanstore.Writer, error) {
			return store.NewSpanWriter(logger, db, indexTable, spansTable, encoding, delay, size), nil
		},
	}
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.logger = logger

	db, err := f.connect(f.Options.getPrimary())
	if err != nil {
		return fmt.Errorf("error connecting to primary db: %v", err)
	}

	f.db = db

	archiveConfig := f.Options.others[archiveNamespace]
	if archiveConfig.Enabled {
		archive, err := f.connect(archiveConfig)
		if err != nil {
			return fmt.Errorf("error connecting to archive db: %v", err)
		}

		f.archive = archive
	}

	return nil
}

func (f *Factory) connect(cfg *namespaceConfig) (*sql.DB, error) {
	if cfg.Encoding != store.EncodingJSON && cfg.Encoding != store.EncodingProto {
		return nil, fmt.Errorf("unknown encoding %q, supported: %q, %q", cfg.Encoding, store.EncodingJSON, store.EncodingProto)
	}

	return cfg.Connector(cfg)
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	cfg := f.Options.getPrimary()
	return f.makeReader(f.db, cfg.OperationsTable, cfg.IndexTable, cfg.SpansTable)
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	cfg := f.Options.getPrimary()
	return f.makeWriter(f.logger, f.db, cfg.IndexTable, cfg.SpansTable, cfg.Encoding, cfg.WriteBatchDelay, cfg.WriteBatchSize)
}

// CreateArchiveSpanReader implements storage.ArchiveFactory
func (f *Factory) CreateArchiveSpanReader() (spanstore.Reader, error) {
	if f.archive == nil {
		return nil, nil
	}
	cfg := f.Options.others[archiveNamespace]
	return f.makeReader(f.archive, cfg.OperationsTable, cfg.IndexTable, cfg.SpansTable)
}

// CreateArchiveSpanWriter implements storage.ArchiveFactory
func (f *Factory) CreateArchiveSpanWriter() (spanstore.Writer, error) {
	if f.archive == nil {
		return nil, nil
	}
	cfg := f.Options.others[archiveNamespace]
	return f.makeWriter(f.logger, f.archive, "", cfg.SpansTable, cfg.Encoding, cfg.WriteBatchDelay, cfg.WriteBatchSize)
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	spanReader, _ := f.CreateSpanReader() // err is always nil
	return dependencyStore.NewDependencyStore(spanReader), nil
}

// Close Implements io.Closer and closes the underlying storage
func (f *Factory) Close() error {
	if f.db != nil {
		err := f.db.Close()
		if err != nil {
			return err
		}

		f.db = nil
	}

	if f.archive != nil {
		err := f.archive.Close()
		if err != nil {
			return err
		}

		f.archive = nil
	}

	return nil
}
