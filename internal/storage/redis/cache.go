// Package redis provides a Redis-backed cache used in Nexus from Ch04 onwards.
//
// Ch04: tenant response caching — demonstrates read-path acceleration.
// Ch10: leader election via Redis SET NX (see internal/election/).
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrCacheMiss is returned when a key is not found in the cache.
var ErrCacheMiss = errors.New("cache miss")

// Cache wraps a Redis client with typed JSON get/set helpers.
// The generic approach keeps call-sites clean while keeping serialisation
// logic in one place.
type Cache struct {
	client *redis.Client
}

// NewCache connects to Redis at the given DSN and pings it.
// DSN format: "redis://user:pass@host:port/db"
func NewCache(ctx context.Context, dsn string) (*Cache, error) {
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("redis: parse DSN: %w", err)
	}

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	return &Cache{client: client}, nil
}

// Close shuts down the Redis connection.
func (c *Cache) Close() error {
	return c.client.Close()
}

// Client returns the underlying Redis client.
// Ch10: used by the election.Elector, which needs direct Redis access
// for atomic Lua scripts that SET NX / PEXPIRE.
func (c *Cache) Client() *redis.Client {
	return c.client
}

// Ping checks the Redis connection. Used by the /ready health probe.
func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Set serialises v as JSON and stores it at key with the given TTL.
func Set[T any](ctx context.Context, c *Cache, key string, v T, ttl time.Duration) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cache set marshal: %w", err)
	}
	return c.client.Set(ctx, key, b, ttl).Err()
}

// Get retrieves the value at key and deserialises it into T.
// Returns ErrCacheMiss if the key does not exist.
func Get[T any](ctx context.Context, c *Cache, key string) (T, error) {
	var zero T
	b, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return zero, ErrCacheMiss
	}
	if err != nil {
		return zero, fmt.Errorf("cache get: %w", err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return zero, fmt.Errorf("cache get unmarshal: %w", err)
	}
	return v, nil
}

// Delete removes a key from the cache. Used on write-through invalidation.
func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}
