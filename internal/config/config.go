package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Validate fails fast on configuration that's safe for development but
// dangerous in production. Currently checks CORS — the defaults include
// the literal "null" origin so the SDK demo page works when opened via
// file://, and that combined with Access-Control-Allow-Credentials:true
// (which the CORS middleware sets unconditionally for allowed origins)
// would let any local HTML file make credentialed requests to nexus.
// Acceptable in dev/workshops, never in production.
//
// Called from main before any network listener starts so a misconfigured
// deploy crashes loud rather than running insecurely.
func (c *Config) Validate() error {
	if !strings.EqualFold(c.Env, "production") {
		return nil
	}
	for _, o := range c.CORSAllowedOrigins {
		switch o {
		case "*":
			return fmt.Errorf("config: CORS_ALLOWED_ORIGINS contains \"*\" — refused in production (Access-Control-Allow-Credentials is unconditionally true, see internal/api/middleware/cors.go)")
		case "null":
			return fmt.Errorf("config: CORS_ALLOWED_ORIGINS contains \"null\" — refused in production (would permit any file:// origin)")
		}
	}
	return nil
}

// Config holds all application configuration.
// Chapter 01: Basic config — just env and address.
// Later chapters will add DB, Redis, Kafka, etc.
type Config struct {
	Env  string
	Addr string

	// Chapter 03+: Primary database
	PostgresDSN string

	// Chapter 04+: Cache + search + OLAP
	RedisDSN         string
	ElasticsearchURL string
	ClickHouseDSN    string

	// Chapter 05+: Encoding
	SchemaRegistryURL string

	// Chapter 06+: Replication
	PostgresReplicaDSN string

	// Chapter 07+: Sharding
	ShardCount int

	// Ch07: when set, Nexus runs in sharded mode. The value is a
	// DSN *template* — Router.ShardDSN appends the shard suffix to
	// the database-name path of this DSN to derive each shard's
	// connection string. Leaving this empty keeps the
	// single-Postgres path that all earlier chapters used.
	ShardDSNTemplate string

	// Chapter 08+: Temporal
	TemporalHostPort string

	// Chapter 12+: Streaming
	KafkaBrokers []string

	// Appendix A (JS SDK): comma-separated list of origins permitted to
	// call the API from a browser. Defaults permit the local SDK demo
	// page (file:// is sent as "null") and a typical dev server. Set
	// "*" to allow any origin — useful for tutorial workshops, never
	// for production.
	CORSAllowedOrigins []string

	// Production-hardening: bootstrap admin API key. When set, it is
	// hashed and upserted as an admin-scoped key at startup so an
	// operator has a credential to mint per-tenant keys via
	// POST /api/v1/admin/api-keys. Leave empty if you seed keys another
	// way (e.g. directly in the database). Never log the raw value.
	BootstrapAdminKey string

	// Production-hardening: per-tenant rate limiting. On by default;
	// limits are sized by each tenant's plan. Set RATE_LIMIT_ENABLED=false
	// to disable (e.g. for load tests or workshops).
	RateLimitEnabled bool
}

func Load() *Config {
	return &Config{
		Env:  getEnv("APP_ENV", "development"),
		Addr: getEnv("APP_ADDR", ":8000"),

		PostgresDSN:        getEnv("POSTGRES_DSN", "postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable"),
		RedisDSN:           getEnv("REDIS_DSN", "redis://localhost:6379/0"),
		ElasticsearchURL:   getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ClickHouseDSN:      getEnv("CLICKHOUSE_DSN", "clickhouse://nexus:nexus_secret@localhost:9000/nexus"),
		PostgresReplicaDSN: getEnv("POSTGRES_REPLICA_DSN", ""),
		SchemaRegistryURL:  getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		ShardCount:         getEnvInt("SHARD_COUNT", 4),
		ShardDSNTemplate:   getEnv("SHARD_DSN_TEMPLATE", ""),
		// 127.0.0.1, not localhost: on Windows + Docker Desktop,
		// localhost resolves [::1] first and the Temporal gRPC
		// frontend only binds the IPv4 side of the published port.
		// The IPv6 attempt hangs until context timeout, surfacing
		// as "temporal: not available" on a perfectly healthy
		// stack. Force IPv4 to skip the resolution.
		TemporalHostPort:   getEnv("TEMPORAL_HOST_PORT", "127.0.0.1:7233"),
		KafkaBrokers:       []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		CORSAllowedOrigins: splitCSV(getEnv("CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173,http://localhost:8080,null")),
		BootstrapAdminKey:  getEnv("NEXUS_BOOTSTRAP_ADMIN_KEY", ""),
		RateLimitEnabled:   getEnvBool("RATE_LIMIT_ENABLED", true),
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := []string{}
	for p := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			parts = append(parts, t)
		}
	}
	return parts
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
