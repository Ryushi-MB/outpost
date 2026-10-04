package deadlinegate

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests run the admission read against MB Wallet's real schema and the real
// read-only role (mb-wallet-neon drizzle/0337 and 0341): a local stack's database. Fixture
// rows are written as the database owner; the read itself runs as
// mbwallet_neon_webhook_gate through PG, the gate's own Admitter.
//
//	MB_GATE_TEST_ADMIN_URL  a superuser or owner URL of the stack database
//	MB_GATE_TEST_GATE_URL   the same database as mbwallet_neon_webhook_gate
func gateDatabases(t *testing.T) (*pgxpool.Pool, *PG) {
	t.Helper()
	adminURL, gateURL := os.Getenv("MB_GATE_TEST_ADMIN_URL"), os.Getenv("MB_GATE_TEST_GATE_URL")
	if adminURL == "" || gateURL == "" {
		t.Skip("MB_GATE_TEST_ADMIN_URL and MB_GATE_TEST_GATE_URL are unset: the admission read needs MB Wallet's schema")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	gate, err := NewPG(ctx, gateURL)
	if err != nil {
		t.Fatalf("gate pool: %v", err)
	}
	t.Cleanup(gate.Close)
	var who string
	if err := gate.pool.QueryRow(ctx, "SELECT current_user").Scan(&who); err != nil || who != "mbwallet_neon_webhook_gate" {
		t.Fatalf("the gate URL connects as %q (%v), want mbwallet_neon_webhook_gate", who, err)
	}
	return admin, gate
}

type accessRow struct {
	state  string // "" means no access row at all
	marker string // "" means NULL
}

type startRecord struct {
	kind         string
	deadlineIn   time.Duration // D relative to the database clock; negative is past
	accessState  string
	marker       string
	staleToken   bool // lease token one below the stop job's current token
	timeoutMs    int
	opposite     bool // ask about the opposite mode's tenant
	otherAgency  bool // ask about another agency's tenant
	upperTenant  bool // ask with the agency id in upper case
	askKind      string
	shiftSignedD bool // ask with D one microsecond later than stored
	unknownID    bool
}

func TestAdmissionRead(t *testing.T) {
	admin, gate := gateDatabases(t)
	ctx := context.Background()

	var agency string
	if err := admin.QueryRow(ctx, "SELECT id FROM orgs WHERE kind = 'agency' ORDER BY created_at LIMIT 1").Scan(&agency); err != nil {
		t.Fatalf("no agency org in the stack database (run stack seed): %v", err)
	}
	// Restore the agency's access row and the lease as they were.
	var hadRow bool
	var prevState string
	var prevMarker *string
	err := admin.QueryRow(ctx, "SELECT state, cycle_marker::text FROM agency_api_access WHERE org_id = $1", agency).Scan(&prevState, &prevMarker)
	hadRow = err == nil
	if err != nil && err != pgx.ErrNoRows {
		t.Fatal(err)
	}
	var token int64
	if err := admin.QueryRow(ctx, "UPDATE stop_job_lease SET fencing_token = fencing_token + 3 RETURNING fencing_token").Scan(&token); err != nil {
		t.Fatal(err)
	}
	var made []string
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DELETE FROM outbound_start_records WHERE id = ANY($1::uuid[])", made)
		if hadRow {
			_, _ = admin.Exec(ctx, "UPDATE agency_api_access SET state = $2, cycle_marker = $3::uuid WHERE org_id = $1", agency, prevState, prevMarker)
		} else {
			_, _ = admin.Exec(ctx, "DELETE FROM agency_api_access WHERE org_id = $1", agency)
		}
	})

	const markerA = "11111111-1111-4111-8111-111111111111"
	const markerB = "22222222-2222-4222-8222-222222222222"
	setAccess := func(t *testing.T, a accessRow) {
		t.Helper()
		if a.state == "" {
			if _, err := admin.Exec(ctx, "DELETE FROM agency_api_access WHERE org_id = $1", agency); err != nil {
				t.Fatal(err)
			}
			return
		}
		var marker any
		if a.marker != "" {
			marker = a.marker
		}
		if _, err := admin.Exec(ctx, `
			INSERT INTO agency_api_access (org_id, state, cycle_marker) VALUES ($1, $2, $3::uuid)
			ON CONFLICT (org_id) DO UPDATE SET state = EXCLUDED.state, cycle_marker = EXCLUDED.cycle_marker`,
			agency, a.state, marker); err != nil {
			t.Fatal(err)
		}
	}
	insert := func(t *testing.T, r startRecord) Admission {
		t.Helper()
		jobKind := strings.HasPrefix(r.kind, "job_")
		var accessState, marker, lease any
		if jobKind {
			accessState, marker = r.accessState, r.marker
		}
		if r.kind != "proxied_mutation" && r.kind != "redrive" {
			lease = token
			if r.staleToken {
				lease = token - 1
			}
		}
		timeout := r.timeoutMs
		if timeout == 0 {
			timeout = 10000
		}
		var id, deadline string
		err := admin.QueryRow(ctx, `
			INSERT INTO outbound_start_records
			  (org_id, mode, kind, created_at, deadline, lease_token, access_state, cycle_marker, request_timeout_ms)
			VALUES ($1, 'live', $2, clock_timestamp() - interval '1 hour',
			        date_trunc('microseconds', clock_timestamp() + $3 * interval '1 millisecond'),
			        $4, $5, $6::uuid, $7)
			RETURNING id::text, to_char(deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
			agency, r.kind, r.deadlineIn.Milliseconds(), lease, accessState, marker, timeout).Scan(&id, &deadline)
		if err != nil {
			t.Fatalf("insert start record: %v", err)
		}
		made = append(made, id)
		a := Admission{StartRecordID: id, Kind: r.kind, Deadline: deadline, Tenant: agency + ":live"}
		if r.askKind != "" {
			a.Kind = r.askKind
		}
		if r.opposite {
			a.Tenant = agency + ":test"
		}
		if r.otherAgency {
			a.Tenant = "6f1c2b8e-3d4a-4e5f-9a0b-1c2d3e4f5a6b:live"
		}
		if r.upperTenant {
			a.Tenant = strings.ToUpper(agency) + ":live"
		}
		if r.shiftSignedD {
			d, _ := time.Parse(DeadlineLayout, deadline)
			a.Deadline = d.Add(time.Microsecond).Format(DeadlineLayout)
		}
		if r.unknownID {
			a.StartRecordID = "0b5f1e2a-7c3d-4e8f-9a1b-2c3d4e5f6a7b"
		}
		return a
	}

	future, past := time.Hour, -time.Minute
	on := accessRow{state: "on"}
	stopping := accessRow{state: "stopping", marker: markerA}
	enabling := accessRow{state: "enabling", marker: markerA}
	cases := []struct {
		name   string
		access accessRow
		record startRecord
		admit  bool
	}{
		{"publish while On before D", on, startRecord{kind: "publish", deadlineIn: future}, true},
		{"redrive while On before D", on, startRecord{kind: "redrive", deadlineIn: future}, true},
		{"republish while On before D", on, startRecord{kind: "republish", deadlineIn: future}, true},
		{"proxied mutation while On before D", on, startRecord{kind: "proxied_mutation", deadlineIn: future}, true},
		{"publish while Off", accessRow{state: "off"}, startRecord{kind: "publish", deadlineIn: future}, false},
		{"publish while Enabling", enabling, startRecord{kind: "publish", deadlineIn: future}, false},
		{"publish while Stopping", stopping, startRecord{kind: "publish", deadlineIn: future}, false},
		{"proxied mutation while Stopping", stopping, startRecord{kind: "proxied_mutation", deadlineIn: future}, false},
		{"publish with no access row", accessRow{}, startRecord{kind: "publish", deadlineIn: future}, false},
		{"publish after D", on, startRecord{kind: "publish", deadlineIn: past}, false},
		{"publish one millisecond after D", on, startRecord{kind: "publish", deadlineIn: -time.Millisecond}, false},

		{"disable in its Stopping cycle", stopping, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA}, true},
		{"list in its Stopping cycle", stopping, startRecord{kind: "job_list", deadlineIn: future, accessState: "stopping", marker: markerA}, true},
		{"list in its Enabling cycle", enabling, startRecord{kind: "job_list", deadlineIn: future, accessState: "enabling", marker: markerA}, true},
		{"enable in its Enabling cycle", enabling, startRecord{kind: "job_enable", deadlineIn: future, accessState: "enabling", marker: markerA}, true},
		{"disable from an earlier cycle", accessRow{state: "stopping", marker: markerB}, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA}, false},
		{"disable delayed past a later turn-on", on, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA}, false},
		{"disable delayed into a later Enabling", accessRow{state: "enabling", marker: markerB}, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA}, false},
		{"disable by a runner with a stale token", stopping, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA, staleToken: true}, false},
		{"disable replayed after D in its own cycle", stopping, startRecord{kind: "job_disable", deadlineIn: past, accessState: "stopping", marker: markerA}, false},
		{"enable once Stopping began", accessRow{state: "stopping", marker: markerA}, startRecord{kind: "job_enable", deadlineIn: future, accessState: "enabling", marker: markerA}, false},
		{"list in Enabling after the cycle moved to Stopping", stopping, startRecord{kind: "job_list", deadlineIn: future, accessState: "enabling", marker: markerA}, false},
		{"publish stale lease is not the gate's check", on, startRecord{kind: "publish", deadlineIn: future, staleToken: true}, true},

		{"signed kind differs from the stored kind", on, startRecord{kind: "publish", deadlineIn: future, askKind: "proxied_mutation"}, false},
		{"job kind asked for a publish record", stopping, startRecord{kind: "publish", deadlineIn: future, askKind: "job_disable"}, false},
		{"opposite mode of the same agency", on, startRecord{kind: "publish", deadlineIn: future, opposite: true}, false},
		{"another agency's tenant", on, startRecord{kind: "publish", deadlineIn: future, otherAgency: true}, false},
		{"agency id in upper case", on, startRecord{kind: "publish", deadlineIn: future, upperTenant: true}, false},
		{"signed D one microsecond off the stored D", on, startRecord{kind: "publish", deadlineIn: future, shiftSignedD: true}, false},
		{"no such start record", on, startRecord{kind: "publish", deadlineIn: future, unknownID: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setAccess(t, tc.access)
			a := insert(t, tc.record)
			d, err := gate.Admit(ctx, a)
			if err != nil {
				t.Fatalf("Admit: %v", err)
			}
			if d.Admit != tc.admit {
				t.Fatalf("admit = %v, want %v", d.Admit, tc.admit)
			}
		})
	}

	t.Run("returns the request timeout stored on the start record", func(t *testing.T) {
		setAccess(t, on)
		d, err := gate.Admit(ctx, insert(t, startRecord{kind: "publish", deadlineIn: future, timeoutMs: 7250}))
		if err != nil || !d.Admit || d.RequestTimeout != 7250*time.Millisecond {
			t.Fatalf("got %+v, %v; want admitted with 7.25s", d, err)
		}
	})
}

func TestAdmissionReadFailsClosed(t *testing.T) {
	ctx := context.Background()
	cases := map[string]string{
		"empty URL":         "",
		"nothing listening": "postgres://mbwallet_neon_webhook_gate@127.0.0.1:1/x?connect_timeout=2",
		"no such host":      "postgres://mbwallet_neon_webhook_gate@gate-db.invalid:5432/x?connect_timeout=2",
		"no such database":  "postgres://mbwallet_neon_webhook_gate@127.0.0.1:5432/mb_gate_no_such_db?connect_timeout=2",
	}
	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			pg, err := NewPG(ctx, url)
			if url == "" && err == nil {
				pg.Close()
				t.Fatal("NewPG accepted an empty URL, which libpq defaults would fill from the host's environment")
			}
			if err != nil {
				return // refused at construction: the gate never starts
			}
			defer pg.Close()
			d, err := pg.Admit(ctx, Admission{StartRecordID: "0b5f1e2a-7c3d-4e8f-9a1b-2c3d4e5f6a7b", Kind: "publish", Deadline: "2999-01-01T00:00:00.000000Z", Tenant: "x:live"})
			if err == nil || d.Admit {
				t.Fatalf("got %+v, %v: an unreachable database must be an error and no admission", d, err)
			}
		})
	}
}
