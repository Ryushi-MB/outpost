package deadlinegate

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// admitSQL is the gate's one statement (task 9.14). It reads only the columns
// mbwallet_neon_webhook_gate holds SELECT on (mb-wallet-neon drizzle/0341) and answers
// true only when the start record exists, its stored kind equals the signed kind, its
// stored D equals the signed D, its tenant "<agency>:<mode>" equals the one the call
// acts on, the database clock is before D, and the kind's own condition holds: access On
// for a publish, redrive, republish or proxied mutation; for a job call the access state
// and cycle marker it was written under, and the stop job lease's current fencing token.
// A replica answers false. Any other outcome refuses the call.
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
               ELSE false
             END,
         false),
       s.request_timeout_ms
  FROM outbound_start_records s
  LEFT JOIN agency_api_access a ON a.org_id = s.org_id
  LEFT JOIN stop_job_lease l ON true
 WHERE s.id = $1::uuid`

// PG is the admission read against MB Wallet's primary, as mbwallet_neon_webhook_gate,
// over a direct connection.
type PG struct{ pool *pgxpool.Pool }

func NewPG(ctx context.Context, url string) (*PG, error) {
	if url == "" {
		return nil, errors.New("deadline gate: WEBHOOK_DEADLINE_GATE_DATABASE_URL must be set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PG{pool: pool}, nil
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
