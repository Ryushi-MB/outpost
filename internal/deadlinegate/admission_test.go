package deadlinegate

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
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
//	MB_GATE_TEST_STANDBY_URL  a streaming standby of that database, as mbwallet_neon_webhook_gate
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
	gate, err := NewPG(ctx, gateURL, true)
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
	mode         string        // the stored mode; "" is live
	deadlineIn   time.Duration // D relative to the database clock; negative is past
	accessState  string
	marker       string
	staleToken   bool // the stop job's lease was taken again after the record was written
	leaseExpired bool // the stop job's lease expired a second ago, D still ahead
	timeoutMs    int
	askMode      string // ask about this mode's tenant; "" is the stored mode
	otherAgency  bool   // ask about another agency's tenant
	upperTenant  bool   // ask with the agency id in upper case
	askKind      string
	shiftSignedD bool // ask with D one microsecond later than stored
	unknownID    bool
	ended        bool // MB Wallet committed the call's end record (ended_at, outcome)
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
	// The stop job holds the lease for the next hour, as it does while it runs; a new take
	// raises the token.
	var token int64
	if err := admin.QueryRow(ctx, `UPDATE stop_job_lease SET fencing_token = fencing_token + 3, holder = 'admission-test',
		expires_at = clock_timestamp() + interval '1 hour' RETURNING fencing_token`).Scan(&token); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "UPDATE stop_job_lease SET fencing_token = fencing_token + 1, holder = NULL, expires_at = NULL")
	})
	// A publish or redrive start record commits only under the publisher's current lease token
	// (mb-wallet-neon drizzle/0354, outbound_start_records_recheck), so the fixture holds the
	// publisher lease too. The gate never compares that lease; the cases below say so.
	var publisherToken int64
	if err := admin.QueryRow(ctx, `UPDATE publisher_lease SET fencing_token = fencing_token + 1, holder = 'admission-test',
		expires_at = clock_timestamp() + interval '1 hour' RETURNING fencing_token`).Scan(&publisherToken); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "UPDATE publisher_lease SET expires_at = clock_timestamp() WHERE holder = 'admission-test'")
	})
	expireLease := func(t *testing.T) {
		t.Helper()
		if _, err := admin.Exec(ctx, "UPDATE stop_job_lease SET expires_at = clock_timestamp() - interval '1 second'"); err != nil {
			t.Fatal(err)
		}
		// An expired lease is taken again only with a higher token (the lease trigger), so the
		// restore is a new take and the later cases sign with its token.
		t.Cleanup(func() {
			if err := admin.QueryRow(ctx, `UPDATE stop_job_lease SET fencing_token = fencing_token + 1,
				expires_at = clock_timestamp() + interval '1 hour' RETURNING fencing_token`).Scan(&token); err != nil {
				t.Errorf("retake the lease: %v", err)
			}
		})
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
		switch {
		case r.kind == "publish" || r.kind == "redrive":
			lease = publisherToken
		case r.kind != "proxied_mutation":
			lease = token
		}
		timeout := r.timeoutMs
		if timeout == 0 {
			timeout = 10000
		}
		mode := r.mode
		if mode == "" {
			mode = "live"
		}
		var id, deadline string
		// A start record commits only in a state that allows it (mb-wallet-neon drizzle/0351:
		// access On, or for a job call its own state and cycle with the lease current), so it is
		// written in that state; each case then moves access or the lease to its condition.
		if jobKind {
			setAccess(t, accessRow{state: r.accessState, marker: r.marker})
		} else {
			setAccess(t, accessRow{state: "on"})
		}
		// D = created_at + the stored send window (0351); created an hour back as before.
		window := (time.Hour + r.deadlineIn).Milliseconds()
		err := admin.QueryRow(ctx, `
			WITH t AS (SELECT date_trunc('microseconds', clock_timestamp()) - interval '1 hour' AS created)
			INSERT INTO outbound_start_records
			  (org_id, mode, kind, created_at, deadline, lease_token, access_state, cycle_marker, request_timeout_ms,
			   transaction_timeout_ms, proxy_call_timeout_ms)
			SELECT $1, $8, $2, t.created, t.created + $3 * interval '1 millisecond', $4, $5, $6::uuid, $7, 5000, $3 FROM t
			RETURNING id::text, to_char(deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
			agency, r.kind, window, lease, accessState, marker, timeout, mode).Scan(&id, &deadline)
		if err != nil {
			t.Fatalf("insert start record: %v", err)
		}
		made = append(made, id)
		if r.staleToken {
			if err := admin.QueryRow(ctx, "UPDATE stop_job_lease SET fencing_token = fencing_token + 1 RETURNING fencing_token").Scan(&token); err != nil {
				t.Fatal(err)
			}
		}
		if r.ended {
			if _, err := admin.Exec(ctx, "UPDATE outbound_start_records SET ended_at = clock_timestamp(), outcome = 'applied' WHERE id = $1::uuid", id); err != nil {
				t.Fatalf("end start record: %v", err)
			}
		}
		a := Admission{StartRecordID: id, Kind: r.kind, Deadline: deadline, Tenant: agency + ":" + mode}
		if r.askKind != "" {
			a.Kind = r.askKind
		}
		if r.askMode != "" {
			a.Tenant = agency + ":" + r.askMode
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
		{"disable by the lease holder after its lease expired, before D", stopping, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA, leaseExpired: true}, false},
		{"list by the lease holder after its lease expired, before D", stopping, startRecord{kind: "job_list", deadlineIn: future, accessState: "stopping", marker: markerA, leaseExpired: true}, false},
		{"enable by the lease holder after its lease expired, before D", enabling, startRecord{kind: "job_enable", deadlineIn: future, accessState: "enabling", marker: markerA, leaseExpired: true}, false},
		{"publish expired lease is not the gate's check", on, startRecord{kind: "publish", deadlineIn: future, leaseExpired: true}, true},

		{"signed kind differs from the stored kind", on, startRecord{kind: "publish", deadlineIn: future, askKind: "proxied_mutation"}, false},
		{"job kind asked for a publish record", stopping, startRecord{kind: "publish", deadlineIn: future, askKind: "job_disable"}, false},
		{"live record asked for the same agency's test tenant", on, startRecord{kind: "publish", deadlineIn: future, askMode: "test"}, false},
		{"test record asked for the same agency's live tenant", on, startRecord{kind: "publish", mode: "test", deadlineIn: future, askMode: "live"}, false},
		{"test record for its own test tenant", on, startRecord{kind: "publish", mode: "test", deadlineIn: future}, true},
		{"another agency's tenant", on, startRecord{kind: "publish", deadlineIn: future, otherAgency: true}, false},
		{"agency id in upper case", on, startRecord{kind: "publish", deadlineIn: future, upperTenant: true}, false},
		{"signed D one microsecond off the stored D", on, startRecord{kind: "publish", deadlineIn: future, shiftSignedD: true}, false},
		{"no such start record", on, startRecord{kind: "publish", deadlineIn: future, unknownID: true}, false},

		{"proxied mutation after its end record, before D", on, startRecord{kind: "proxied_mutation", deadlineIn: future, ended: true}, false},
		{"publish after its end record, before D", on, startRecord{kind: "publish", deadlineIn: future, ended: true}, false},
		{"disable after its end record, in its Stopping cycle before D", stopping, startRecord{kind: "job_disable", deadlineIn: future, accessState: "stopping", marker: markerA, ended: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := insert(t, tc.record)
			setAccess(t, tc.access)
			if tc.record.leaseExpired {
				expireLease(t)
			}
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

	// The gate's database address can come to point at a standby after the gate started
	// (a failover behind one host name). The relay stands in for that address: first in
	// front of the primary, then of the standby. The per-call replica check must refuse.
	t.Run("a standby behind the gate's address answers false", func(t *testing.T) {
		gateURL, standbyURL := os.Getenv("MB_GATE_TEST_GATE_URL"), os.Getenv("MB_GATE_TEST_STANDBY_URL")
		if standbyURL == "" {
			// ablate.sh refuses to run without it, so the mutation rows never count this skip.
			t.Skip("MB_GATE_TEST_STANDBY_URL is unset: this check needs a streaming standby of the test database")
		}
		r := newRelay(t, urlHost(t, gateURL))
		pg, err := NewPG(ctx, withURLHost(t, gateURL, r.ln.Addr().String()), true)
		if err != nil {
			t.Fatalf("NewPG through the relay to the primary: %v", err)
		}
		defer pg.Close()
		setAccess(t, on)
		a := insert(t, startRecord{kind: "publish", deadlineIn: future})
		if d, err := pg.Admit(ctx, a); err != nil || !d.Admit {
			t.Fatalf("the primary answered %+v, %v; want admitted", d, err)
		}

		var lsn string
		if err := admin.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
			t.Fatal(err)
		}
		standby, err := pgx.Connect(ctx, standbyURL)
		if err != nil {
			t.Fatalf("standby: %v", err)
		}
		defer standby.Close(ctx)
		for waited := time.Duration(0); ; waited += 50 * time.Millisecond {
			var replayed bool
			if err := standby.QueryRow(ctx, "SELECT pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= $1::pg_lsn", lsn).Scan(&replayed); err != nil {
				t.Fatal(err)
			}
			if replayed {
				break
			}
			if waited > 10*time.Second {
				t.Fatal("the standby did not replay the start record within 10s")
			}
			time.Sleep(50 * time.Millisecond)
		}

		r.switchTo(urlHost(t, standbyURL))
		var d Decision
		for i := 0; ; i++ { // the first call may meet a connection the switch closed
			if d, err = pg.Admit(ctx, a); err == nil {
				break
			}
			if i == 5 {
				t.Fatalf("no answer through the relay from the standby: %v", err)
			}
		}
		if d.Admit {
			t.Fatal("a standby admitted a call the primary admits")
		}
	})
}

// NewPG refuses to start the gate on a database it cannot reach, or on a connection that
// is not a verified TLS connection to the primary as the gate role. Each case asserts the
// reason, so a refusal by another check does not count.
func TestNewPGRefusesAnUnsafeDatabase(t *testing.T) {
	ctx := context.Background()
	type unsafeDB struct {
		name, url string
		local     bool
		reason    string
	}
	cases := []unsafeDB{
		{"empty URL", "", true, "must be set"},
		{"nothing listening", "postgres://mbwallet_neon_webhook_gate@127.0.0.1:1/x?sslmode=disable&connect_timeout=2", true, "did not answer"},
		{"no such host", "postgres://mbwallet_neon_webhook_gate@gate-db.invalid:5432/x?sslmode=verify-full&connect_timeout=2", false, "did not answer"},
		{"plaintext", "postgres://mbwallet_neon_webhook_gate@db.example.com/x?sslmode=disable", false, "sslmode=verify-full"},
		{"no sslmode, which falls back to plaintext", "postgres://mbwallet_neon_webhook_gate@db.example.com/x", false, "sslmode=verify-full"},
		{"sslmode=require, which skips the certificate", "postgres://mbwallet_neon_webhook_gate@db.example.com/x?sslmode=require", false, "sslmode=verify-full"},
		{"sslmode=verify-ca, which skips the host name", "postgres://mbwallet_neon_webhook_gate@db.example.com/x?sslmode=verify-ca", false, "sslmode=verify-full"},
		{"the local waiver for a remote host", "postgres://mbwallet_neon_webhook_gate@db.example.com/x?sslmode=disable", true, "not local"},
		{"the local waiver for a remote second host", "postgres://mbwallet_neon_webhook_gate@127.0.0.1,db.example.com/x?sslmode=disable", true, "not local"},
	}
	if gateURL, adminURL, standbyURL := os.Getenv("MB_GATE_TEST_GATE_URL"), os.Getenv("MB_GATE_TEST_ADMIN_URL"), os.Getenv("MB_GATE_TEST_STANDBY_URL"); gateURL != "" && adminURL != "" && standbyURL != "" {
		cases = append(cases,
			unsafeDB{"the owner role in place of the gate role", adminURL, true, "want " + GateRole},
			unsafeDB{"a standby", standbyURL, true, "is a standby"},
		)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pg, err := NewPG(ctx, tc.url, tc.local)
			if err == nil {
				pg.Close()
				t.Fatal("NewPG started the gate on this database")
			}
			if !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("refused for another reason: %v (want %q)", err, tc.reason)
			}
		})
	}
}

// The configurations the gate is meant to start with do start: the local stack's
// plaintext with the waiver, and verify-full against a server whose certificate the
// URL's sslrootcert names (MB_GATE_TEST_GATE_TLS_URL).
func TestNewPGStartsOnTheGateDatabase(t *testing.T) {
	gateURL, tlsURL := os.Getenv("MB_GATE_TEST_GATE_URL"), os.Getenv("MB_GATE_TEST_GATE_TLS_URL")
	if gateURL == "" || tlsURL == "" {
		t.Skip("MB_GATE_TEST_GATE_URL and MB_GATE_TEST_GATE_TLS_URL are unset")
	}
	for name, c := range map[string]struct {
		url   string
		local bool
	}{"local plaintext": {gateURL, true}, "verify-full": {tlsURL, false}} {
		t.Run(name, func(t *testing.T) {
			pg, err := NewPG(context.Background(), c.url, c.local)
			if err != nil {
				t.Fatalf("NewPG: %v", err)
			}
			pg.Close()
		})
	}
}

// relay is a TCP forwarder whose target can be switched; a switch closes the connections
// it carries, as a failover does.
type relay struct {
	ln     net.Listener
	mu     sync.Mutex
	target string
	conns  []net.Conn
}

func newRelay(t *testing.T, target string) *relay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &relay{ln: ln, target: target}
	t.Cleanup(func() { ln.Close(); r.switchTo("") })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go r.carry(c)
		}
	}()
	return r
}

func (r *relay) carry(c net.Conn) {
	r.mu.Lock()
	target := r.target
	r.mu.Unlock()
	up, err := net.Dial("tcp", target)
	if err != nil {
		c.Close()
		return
	}
	r.mu.Lock()
	r.conns = append(r.conns, c, up)
	r.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); up.Close() }()
	_, _ = io.Copy(c, up)
	c.Close()
}

func (r *relay) switchTo(target string) {
	r.mu.Lock()
	r.target = target
	conns := r.conns
	r.conns = nil
	r.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func urlHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func withURLHost(t *testing.T, raw, host string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Host = host
	return u.String()
}
