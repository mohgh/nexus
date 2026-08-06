// Package clickhouse provides a ClickHouse client for OLAP queries.
//
// Ch04: columnar storage demo — the same events stored in PostgreSQL (OLTP)
//       are also written to ClickHouse (OLAP) to show the performance difference
//       for aggregate queries (COUNT, SUM, GROUP BY across millions of rows).
//
// Ch12: Debezium CDC pipeline writes events to ClickHouse automatically via Kafka.
package clickhouse

import (
	"context"
	"fmt"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client wraps a ClickHouse connection.
type Client struct {
	conn driver.Conn
}

// NewClient opens a ClickHouse connection using the native TCP protocol.
// DSN format: "clickhouse://user:pass@host:9000/database"
//
// ClickHouse is append-only by design — inserts are fast, updates/deletes
// are not. The MergeTree engine batches small inserts asynchronously;
// do not rely on immediate consistency for reads after writes in tests.
func NewClient(ctx context.Context, dsn string) (*Client, error) {
	opts, err := chdriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}

	conn, err := chdriver.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}

	return &Client{conn: conn}, nil
}

// Close shuts down the ClickHouse connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping checks the ClickHouse connection. Used by the /ready health probe.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Conn exposes the raw driver connection for repositories that need it.
func (c *Client) Conn() driver.Conn {
	return c.conn
}
