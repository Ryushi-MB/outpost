package deadlinegate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// admitSQL is the gate's one statement (task 9.14). It reads only the columns
// mbwallet_neon_webhook_gate holds SELECT on (mb-wallet-neon drizzle/0341) and answers
// true only when the start record exists, its stored kind equals the signed kind, its
// stored D equals the signed D, its tenant "<agency>:<mode>" equals the one the call
// acts on, the database clock is before D, and the kind's own condition holds: access On
// for a publish, redrive, republish or proxied mutation; for a job call the access state
// and cycle marker it was written under, the stop job lease's current fencing token, and
// that lease not yet expired by the database clock (a runner whose lease lapsed before D
// is no longer the stop job). The lease columns need SELECT (fencing_token, expires_at)
// for the gate role. A replica answers false. Any other outcome refuses the call.
const admitSQL = `
SELECT COALESCE(
         NOT pg_is_in_recovery()
         AND s.kind = $2
         AND s.deadline = $3::timestamptz
         AND s.org_id::text || ':' || s.mode = $4
         AND clock_timestamp() < s.deadline
         AND CASE
               WHEN s.kind IN ('publish', 'redrive', 'republish', 'proxied_mutation')
                 THEN a.state = 'on'
               WHEN s.kind IN ('job_disable', 'job_list', 'job_enable')
                 THEN a.state = s.access_state
                  AND a.cycle_marker = s.cycle_marker
                  AND s.lease_token = l.fencing_token
                  AND clock_timestamp() < l.expires_at
               ELSE false
             END,
         false),
       s.request_timeout_ms
  FROM outbound_start_records s
  LEFT JOIN agency_api_access a ON a.org_id = s.org_id
  LEFT JOIN stop_job_lease l ON true
 WHERE s.id = $1::uuid`

// GateRole is the only role the gate's database connection may log in as.
const GateRole = "mbwallet_neon_webhook_gate"

// PG is the admission read against MB Wallet's primary, as mbwallet_neon_webhook_gate,
// over a direct connection.
type PG struct{ pool *pgxpool.Pool }

// NewPG connects the admission read and refuses to start unless the connection verifies
// the server's certificate and host name (sslmode=verify-full), every connection logs in
// as GateRole, and the server answers as a primary. localPlaintext waives the certificate
// rule for a local stack only: every host must then be this machine or the Docker host.
// The replica rule is checked again on every call, inside the admission statement.
func NewPG(ctx context.Context, url string, localPlaintext bool) (*PG, error) {
	if url == "" {
		return nil, errors.New("deadline gate: WEBHOOK_DEADLINE_GATE_DATABASE_URL must be set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if err := requireVerifiedTLS(cfg.ConnConfig.Config, localPlaintext); err != nil {
		return nil, err
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		var session, current string
		if err := conn.QueryRow(ctx, "SELECT session_user::text, current_user::text").Scan(&session, &current); err != nil {
			return err
		}
		if session != GateRole || current != GateRole {
			return fmt.Errorf("deadline gate: the gate database connects as %q (current %q), want %s", session, current, GateRole)
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var standby bool
	if err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&standby); err != nil {
		pool.Close()
		return nil, fmt.Errorf("deadline gate: the gate database did not answer at start: %w", err)
	}
	if standby {
		pool.Close()
		return nil, errors.New("deadline gate: the gate database is a standby; it must be the primary")
	}
	return &PG{pool: pool}, nil
}

// requireVerifiedTLS reads the connection settings pgx will actually use (the URL and
// the PG* environment), every host and fallback included.
func requireVerifiedTLS(c pgconn.Config, localPlaintext bool) error {
	targets := append([]*pgconn.FallbackConfig{{Host: c.Host, Port: c.Port, TLSConfig: c.TLSConfig}}, c.Fallbacks...)
	for _, t := range targets {
		if localPlaintext {
			if !localHost(t.Host) {
				return fmt.Errorf("deadline gate: WEBHOOK_DEADLINE_GATE_LOCAL_PLAINTEXT is for a local stack only, and %q is not local", t.Host)
			}
			continue
		}
		if t.TLSConfig == nil || t.TLSConfig.InsecureSkipVerify {
			return errors.New("deadline gate: the gate database URL must use sslmode=verify-full")
		}
	}
	return nil
}

func localHost(h string) bool {
	switch h {
	case "127.0.0.1", "::1", "localhost", "host.docker.internal":
		return true
	}
	return strings.HasPrefix(h, "/") // a Unix socket on this machine
}

func (p *PG) Close() { p.pool.Close() }

func (p *PG) Admit(ctx context.Context, a Admission) (Decision, error) {
	var admit bool
	var timeoutMs int64
	err := p.pool.QueryRow(ctx, admitSQL, a.StartRecordID, a.Kind, a.Deadline, a.Tenant).Scan(&admit, &timeoutMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return Decision{}, nil
	}
	if err != nil {
		return Decision{}, err
	}
	return Decision{Admit: admit, RequestTimeout: time.Duration(timeoutMs) * time.Millisecond}, nil
}
