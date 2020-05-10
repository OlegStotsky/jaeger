# ClickHouse storage backend

## Schema

It's expected that the tables are created externally, not by Jaeger itself. This
allows you to tweak the schema, as long as you preserve raad/write interface.

The schema went through a few iterations. Current version is three tables:

* Index table for searches:

```sql
CREATE TABLE jaeger_index_v2 (
  timestamp DateTime64(6) CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  service LowCardinality(String) CODEC(ZSTD(1)),
  operation LowCardinality(String) CODEC(ZSTD(1)),
  durationUs UInt64 CODEC(ZSTD(1)),
  tags Array(String) CODEC(ZSTD(1)),
  INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (service, timestamp)
SETTINGS index_granularity=1024
```

Here we have a primary key that allows early termination of search by limit,
if you have more hits than you requested. This is very handy if you have a High
rate of spans coming into the system.

Tag lookups are done by bloom filter, so you can narrow down exact matches.

* Span lookup table for models:

```sql
CREATE TABLE jaeger_spans_v2 (
  timestamp DateTime64(6) CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  model String CODEC(ZSTD(3))
) ENGINE MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY traceID
SETTINGS index_granularity=1024
```

Here we have a primary key lookup by `traceID`, but having it sorted
differently from the index table mens that models are not compressed as well.

* Service and operation lookup table:

```sql
CREATE MATERIALIZED VIEW jaeger_operations_v2
ENGINE SummingMergeTree
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
  toDate(timestamp) AS date,
  service,
  operation,
  count() as count
FROM jaeger_index_v2
GROUP BY date, service, operation
```

Here we keep our services and operations in a compact form, since it's rather
expensive to pull them out of the index table, compared to their cardinality.

## Archive

Archive table for spans is the same as regular span storage, but with a monthly
partition key, since the number of archived traces is unlikely to be large:

```sql
CREATE TABLE jaeger_archive_spans_v2 (
  timestamp DateTime64(6) CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  model String CODEC(ZSTD(3))
) ENGINE MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY traceID
SETTINGS index_granularity=1024
```

### Other tried schema approaches

#### Tags in a nested field

Having `tags` like this makes searches way slower:

```
tags Nested(
  key LowCardinality(String),
  valueString LowCardinality(String),
  valueBool UInt8,
  valueInt Int64,
  valueFloat Float64
)
```

However, it allows more complex searches, like prefix for strings and range
for numeric values.

#### Single table

One other approach is to have a single table with an extra index on `traceID`:

```sql
CREATE TABLE jaeger_v1 (
  timestamp DateTime64(6) CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  service LowCardinality(String) CODEC(ZSTD(1)),
  operation LowCardinality(String) CODEC(ZSTD(1)),
  durationUs UInt64 CODEC(ZSTD(1)),
  tags Array(String) CODEC(ZSTD(1)),
  model String CODEC(ZSTD(3)),
  INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_traces traceID TYPE bloom_filter(0.01) GRANULARITY 512
) ENGINE MergeTree() PARTITION BY toDate(timestamp) ORDER BY (service, timestamp)
SETTINGS index_granularity=1024;
```

Unfortunately, this is a whole lot slower for lookups by `traceID`.

#### Having `operation` in primary key

Having `ORDER BY (service, operation, timestamp)` means that you fall back
to slower searches when operation is not defined. Having `operation` at the end
doesn't make anything faster. One could hope for better compression to sorting,
but it wasn't observed in test setting.

#### Different codecs for columns

Here are values that were tried (other than the defaults), before settling:

* `timestamp`: `CODEC(DoubleDelta)`, `CODEC(DoubleDelta, ZSTD(1))`
* `durationUs`: `CODEC(T64)`
* `tags`: `CODEC(ZSTD(3))`
* `model`: `CODEC(ZSTD(1))`

#### Different index granularity

Index granularity is picked to be explicit (just like compression level),
and it proved to be a bit faster and less greedy in terms of reads in testing.

Your mileage may vary.

#### Binary traceID column

Having `traceID` as a binary string in theory saves half of uncompressed space,
but with compression enabled it saved only 0.3 bytes per span.
