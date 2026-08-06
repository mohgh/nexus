// Package anomalies hosts runnable demonstrations of the classical
// transaction concurrency anomalies discussed in Chapter 8: lost update
// and write skew. The tests are deliberately structured to *fail* under
// the weak isolation level the chapter critiques and to *pass* once the
// recommended fix is applied — so a student running them sees the
// failure mode and the correction back-to-back.
//
// These tests touch a live PostgreSQL and so are gated behind the
// `integration` build tag. Run them with:
//
//	make ch08-anomalies
//
// or directly:
//
//	POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
//	    go test -tags=integration -v ./internal/transactions/anomalies/...
//
// Each test creates its own scratch table named after the test, runs
// concurrent goroutines that produce or avoid the anomaly, and drops
// the table on the way out — there is no migration churn and no
// pollution of the application schema.
package anomalies
