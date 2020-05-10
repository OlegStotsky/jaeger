package clickhouse

import (
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger/pkg/config"
	"github.com/jaegertracing/jaeger/plugin/storage/clickhouse/spanstore"
)

func TestDefaultOptionsParsing(t *testing.T) {
	opts := NewOptions(primaryNamespace, archiveNamespace)
	v, command := config.Viperize(opts.AddFlags)
	err := command.ParseFlags([]string{})
	assert.NoError(t, err)
	opts.InitFromViper(v)

	primary := opts.getPrimary()

	assert.Equal(t, defaultDatasource, primary.Datasource)
	assert.Equal(t, defaultOperationsTable, primary.OperationsTable)
	assert.Equal(t, defaultIndexTable, primary.IndexTable)
	assert.Equal(t, defaultSpansTable, primary.SpansTable)
	assert.Equal(t, defaultWriteBatchDelay, primary.WriteBatchDelay)
	assert.Equal(t, defaultWriteBatchSize, primary.WriteBatchSize)
	assert.Equal(t, defaultEncoding, primary.Encoding)

	archive, ok := opts.others[archiveNamespace]

	assert.True(t, ok)

	assert.False(t, archive.Enabled)
	assert.Equal(t, defaultDatasource, archive.Datasource)
	assert.Equal(t, "", archive.OperationsTable)
	assert.Equal(t, "", archive.IndexTable)
	assert.Equal(t, defaultArchiveSpansTable, archive.SpansTable)
	assert.Equal(t, defaultWriteBatchDelay, archive.WriteBatchDelay)
	assert.Equal(t, defaultWriteBatchSize, archive.WriteBatchSize)
	assert.Equal(t, defaultEncoding, archive.Encoding)
}

func TestParseOptions(t *testing.T) {
	opts := NewOptions(primaryNamespace, archiveNamespace)
	v, command := config.Viperize(opts.AddFlags)
	err := command.ParseFlags([]string{
		"--clickhouse.datasource=tcp://localhost:9000?debug=true&database=jaeger_primary_huh",
		"--clickhouse.encoding=json",
		"--clickhouse.index-table=jaeger_index_huh",
		"--clickhouse.operations-table=jaeger_operations_huh",
		"--clickhouse.spans-table=jaeger_spans_huh",
		"--clickhouse.write-batch-delay=69ms",
		"--clickhouse.write-batch-size=13",

		"--clickhouse-archive.enabled=true",
		"--clickhouse-archive.datasource=tcp://localhost:9000?debug=true&database=jaeger_archive_heh",
		"--clickhouse-archive.encoding=json",
		"--clickhouse-archive.spans-table=jaeger_archive_spans_heh",
		"--clickhouse-archive.write-batch-delay=1s",
		"--clickhouse-archive.write-batch-size=42",
	})
	assert.NoError(t, err)
	opts.InitFromViper(v)

	primary := opts.getPrimary()

	assert.Equal(t, "tcp://localhost:9000?debug=true&database=jaeger_primary_huh", primary.Datasource)
	assert.Equal(t, "jaeger_operations_huh", primary.OperationsTable)
	assert.Equal(t, "jaeger_index_huh", primary.IndexTable)
	assert.Equal(t, "jaeger_spans_huh", primary.SpansTable)
	assert.Equal(t, time.Millisecond*69, primary.WriteBatchDelay)
	assert.Equal(t, 13, primary.WriteBatchSize)
	assert.Equal(t, spanstore.EncodingJSON, primary.Encoding)

	archive, ok := opts.others[archiveNamespace]

	assert.True(t, ok)

	assert.True(t, archive.Enabled)
	assert.Equal(t, "tcp://localhost:9000?debug=true&database=jaeger_archive_heh", archive.Datasource)
	assert.Equal(t, "", archive.OperationsTable)
	assert.Equal(t, "", archive.IndexTable)
	assert.Equal(t, "jaeger_archive_spans_heh", archive.SpansTable)
	assert.Equal(t, time.Second, archive.WriteBatchDelay)
	assert.Equal(t, 42, archive.WriteBatchSize)
	assert.Equal(t, spanstore.EncodingJSON, archive.Encoding)
}
