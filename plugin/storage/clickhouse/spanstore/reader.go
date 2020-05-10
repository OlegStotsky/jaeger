package spanstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	ErrNoOperationsTable = errors.New("no operations table supplied")
	ErrNoIndexTable      = errors.New("no index table supplied")
)

// SpanWriter for reading spans from ClickHouse
type TraceReader struct {
	db              *sql.DB
	operationsTable string
	indexTable      string
	spansTable      string
}

// NewTraceReader returns a TraceReader for the database
func NewTraceReader(db *sql.DB, operationsTable, indexTable, spansTable string) *TraceReader {
	return &TraceReader{
		db:              db,
		operationsTable: operationsTable,
		indexTable:      indexTable,
		spansTable:      spansTable,
	}
}

func (r *TraceReader) getTraces(ctx context.Context, traceIDs []model.TraceID) ([]*model.Trace, error) {
	returning := make([]*model.Trace, 0, len(traceIDs))

	if len(traceIDs) == 0 {
		return returning, nil
	}

	span, _ := opentracing.StartSpanFromContext(ctx, "getTraces")
	defer span.Finish()

	values := make([]interface{}, len(traceIDs))
	for i, traceID := range traceIDs {
		values[i] = traceID.String()
	}

	query := fmt.Sprintf("SELECT model FROM %s WHERE traceID IN (%s)", r.spansTable, "?"+strings.Repeat(",?", len(values)-1))

	span.SetTag("db.statement", query)
	span.SetTag("db.args", values)

	rows, err := r.db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	traces := map[model.TraceID]*model.Trace{}

	for rows.Next() {
		var serialized string

		if err := rows.Scan(&serialized); err != nil {
			return nil, err
		}

		span := model.Span{}

		if serialized[0] == '{' {
			err = json.Unmarshal([]byte(serialized), &span)
		} else {
			err = proto.Unmarshal([]byte(serialized), &span)
		}

		if err != nil {
			return nil, err
		}

		if _, ok := traces[span.TraceID]; !ok {
			traces[span.TraceID] = &model.Trace{}
		}

		traces[span.TraceID].Spans = append(traces[span.TraceID].Spans, &span)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, traceID := range traceIDs {
		if trace, ok := traces[traceID]; ok {
			returning = append(returning, trace)
		}
	}

	return returning, nil
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (r *TraceReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()

	traces, err := r.getTraces(ctx, []model.TraceID{traceID})
	if err != nil {
		return nil, err
	}

	if len(traces) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return traces[0], nil
}

func (r *TraceReader) getStrings(ctx context.Context, sql string, args ...interface{}) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	values := []string{}

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

// GetServices fetches the sorted service list that have not expired
func (r *TraceReader) GetServices(ctx context.Context) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer span.Finish()

	if r.operationsTable == "" {
		return nil, ErrNoOperationsTable
	}

	query := fmt.Sprintf("SELECT service FROM %s GROUP BY service", r.operationsTable)

	span.SetTag("db.statement", query)

	return r.getStrings(ctx, query)
}

// GetOperations fetches operations in the service and empty slice if service does not exists
func (r *TraceReader) GetOperations(
	ctx context.Context,
	params spanstore.OperationQueryParameters,
) ([]spanstore.Operation, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	if r.operationsTable == "" {
		return nil, ErrNoOperationsTable
	}

	query := fmt.Sprintf("SELECT operation FROM %s WHERE service = ? GROUP BY operation", r.operationsTable)
	args := []interface{}{params.ServiceName}

	span.SetTag("db.statement", query)
	span.SetTag("db.args", args)

	names, err := r.getStrings(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	operations := make([]spanstore.Operation, len(names))
	for i, name := range names {
		operations[i].Name = name
	}

	return operations, nil
}

// FindTraces retrieves traces that match the traceQuery
func (r *TraceReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	traceIDs, err := r.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	return r.getTraces(ctx, traceIDs)
}

// FindTraceIDs retrieves only the TraceIDs that match the traceQuery, but not the trace data
func (r *TraceReader) FindTraceIDs(ctx context.Context, params *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	if r.indexTable == "" {
		return nil, ErrNoIndexTable
	}

	query := fmt.Sprintf("SELECT DISTINCT traceID FROM %s WHERE service = ?", r.indexTable)
	args := []interface{}{params.ServiceName}

	if params.OperationName != "" {
		query = query + " AND operation = ?"
		args = append(args, params.OperationName)
	}

	if !params.StartTimeMin.IsZero() {
		query = query + " AND timestamp >= toDateTime64(?, 6, 'UTC')"
		args = append(args, params.StartTimeMin.UTC().Format("2006-01-02T15:04:05"))
	}

	if !params.StartTimeMax.IsZero() {
		query = query + " AND timestamp <= toDateTime64(?, 6, 'UTC')"
		args = append(args, params.StartTimeMax.UTC().Format("2006-01-02T15:04:05"))
	}

	if params.DurationMin != 0 {
		query = query + " AND durationUs >= ?"
		args = append(args, params.DurationMin.Microseconds())
	}

	if params.DurationMax != 0 {
		query = query + " AND durationUs <= ?"
		args = append(args, params.DurationMax.Microseconds())
	}

	for key, value := range params.Tags {
		query = query + " AND has(tags, ?)"
		args = append(args, fmt.Sprintf("%s=%s", key, value))
	}

	// Sorting by service is required for early termination of primary key scan:
	// * https://github.com/ClickHouse/ClickHouse/issues/7102
	query = query + " ORDER BY service DESC, timestamp DESC LIMIT ?"
	args = append(args, params.NumTraces)

	span.SetTag("db.statement", query)
	span.SetTag("db.args", args)

	traceIDStrings, err := r.getStrings(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, len(traceIDStrings))
	for i, traceIDString := range traceIDStrings {
		traceID, err := model.TraceIDFromString(traceIDString)
		if err != nil {
			return nil, err
		}
		traceIDs[i] = traceID
	}

	return traceIDs, nil
}
