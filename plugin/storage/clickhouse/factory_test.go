package clickhouse

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	assert "github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/config"
	store "github.com/jaegertracing/jaeger/plugin/storage/clickhouse/spanstore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func makeMockConnector() (sqlmock.Sqlmock, Connector, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}

	return mock, func(cfg *namespaceConfig) (*sql.DB, error) {
		return db, err
	}, nil
}

func wrapReaderMaker(t *testing.T, makeReader readerMaker, expectedDB *sql.DB, expectedOperationsTable, expectedIndexTable, expectedSpansTable string) (readerMaker, *bool) {
	called := false

	return func(db *sql.DB, operationsTable, indexTable, spansTable string) (spanstore.Reader, error) {
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedOperationsTable, operationsTable)
		assert.Equal(t, expectedIndexTable, indexTable)
		assert.Equal(t, expectedSpansTable, spansTable)

		called = true

		return makeReader(db, operationsTable, indexTable, spansTable)
	}, &called
}

func wrapWriterMaker(t *testing.T, makeWriter writerMaker, expectedLogger *zap.Logger, expectedDB *sql.DB, expectedIndexTable string, expectedSpansTable string, expectedEncoding store.Encoding, expectedDelay time.Duration, expectedSize int) (writerMaker, *bool) {
	called := false

	return func(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding store.Encoding, delay time.Duration, size int) (spanstore.Writer, error) {
		assert.Equal(t, expectedLogger, logger)
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedIndexTable, indexTable)
		assert.Equal(t, expectedSpansTable, spansTable)
		assert.Equal(t, expectedEncoding, encoding)
		assert.Equal(t, expectedDelay, delay)
		assert.Equal(t, expectedSize, size)

		called = true

		return makeWriter(logger, db, indexTable, spansTable, encoding, delay, size)
	}, &called
}

func TestWithoutArchive(t *testing.T) {
	f := NewFactory()

	_, connector, err := makeMockConnector()
	assert.NoError(t, err)

	f.Options.primary.Connector = connector

	err = f.Initialize(metrics.NullFactory, zap.NewNop())
	assert.NoError(t, err)

	primary := f.Options.primary
	archive := f.Options.others[archiveNamespace]

	originalMakeReader := f.makeReader
	originalMakeWriter := f.makeWriter

	makeReader, makeReaderCalled := wrapReaderMaker(t, originalMakeReader, f.db, primary.OperationsTable, primary.IndexTable, primary.SpansTable)
	f.makeReader = makeReader

	r, err := f.CreateSpanReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.True(t, *makeReaderCalled)

	makeWriter, makeWriterCalled := wrapWriterMaker(t, originalMakeWriter, f.logger, f.db, primary.IndexTable, primary.SpansTable, primary.Encoding, primary.WriteBatchDelay, primary.WriteBatchSize)
	f.makeWriter = makeWriter

	w, err := f.CreateSpanWriter()
	assert.NoError(t, err)
	assert.NotNil(t, w)
	assert.True(t, *makeWriterCalled)

	makeReader, makeReaderCalled = wrapReaderMaker(t, originalMakeReader, f.archive, archive.OperationsTable, archive.IndexTable, archive.SpansTable)
	f.makeReader = makeReader

	ar, err := f.CreateArchiveSpanReader()
	assert.NoError(t, err)
	assert.Nil(t, ar)
	assert.False(t, *makeReaderCalled)

	makeWriter, makeWriterCalled = wrapWriterMaker(t, originalMakeWriter, f.logger, f.archive, "", archive.SpansTable, archive.Encoding, archive.WriteBatchDelay, archive.WriteBatchSize)
	f.makeWriter = makeWriter

	aw, err := f.CreateArchiveSpanWriter()
	assert.NoError(t, err)
	assert.Nil(t, aw)
	assert.False(t, *makeWriterCalled)

	makeReader, makeReaderCalled = wrapReaderMaker(t, originalMakeReader, f.db, primary.OperationsTable, primary.IndexTable, primary.SpansTable)
	f.makeReader = makeReader

	dr, err := f.CreateDependencyReader()
	assert.NoError(t, err)
	assert.NotNil(t, dr)
	assert.True(t, *makeReaderCalled)
}

func TestWithArchive(t *testing.T) {
	f := NewFactory()

	v, command := config.Viperize(f.AddFlags)
	err := command.ParseFlags([]string{"--clickhouse-archive.enabled=true"})
	assert.NoError(t, err)
	f.InitFromViper(v)

	_, connector, err := makeMockConnector()
	assert.NoError(t, err)

	f.Options.primary.Connector = connector
	f.Options.others[archiveNamespace].Connector = connector

	err = f.Initialize(metrics.NullFactory, zap.NewNop())
	assert.NoError(t, err)

	primary := f.Options.primary
	archive := f.Options.others[archiveNamespace]

	originalMakeReader := f.makeReader
	originalMakeWriter := f.makeWriter

	makeReader, makeReaderCalled := wrapReaderMaker(t, originalMakeReader, f.db, primary.OperationsTable, primary.IndexTable, primary.SpansTable)
	f.makeReader = makeReader

	r, err := f.CreateSpanReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.True(t, *makeReaderCalled)

	makeWriter, makeWriterCalled := wrapWriterMaker(t, originalMakeWriter, f.logger, f.db, primary.IndexTable, primary.SpansTable, primary.Encoding, primary.WriteBatchDelay, primary.WriteBatchSize)
	f.makeWriter = makeWriter

	w, err := f.CreateSpanWriter()
	assert.NoError(t, err)
	assert.NotNil(t, w)
	assert.True(t, *makeWriterCalled)

	makeReader, makeReaderCalled = wrapReaderMaker(t, originalMakeReader, f.archive, archive.OperationsTable, archive.IndexTable, archive.SpansTable)
	f.makeReader = makeReader

	ar, err := f.CreateArchiveSpanReader()
	assert.NoError(t, err)
	assert.NotNil(t, ar)
	assert.True(t, *makeReaderCalled)

	makeWriter, makeWriterCalled = wrapWriterMaker(t, originalMakeWriter, f.logger, f.archive, "", archive.SpansTable, archive.Encoding, archive.WriteBatchDelay, archive.WriteBatchSize)
	f.makeWriter = makeWriter

	aw, err := f.CreateArchiveSpanWriter()
	assert.NoError(t, err)
	assert.NotNil(t, aw)
	assert.True(t, *makeWriterCalled)

	makeReader, makeReaderCalled = wrapReaderMaker(t, originalMakeReader, f.db, primary.OperationsTable, primary.IndexTable, primary.SpansTable)
	f.makeReader = makeReader

	dr, err := f.CreateDependencyReader()
	assert.NoError(t, err)
	assert.NotNil(t, dr)
	assert.True(t, *makeReaderCalled)
}
