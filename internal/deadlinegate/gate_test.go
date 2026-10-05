package deadlinegate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	keyCurrent  = []byte("current-deadline-secret-0123456789abcdef")
	keyPrevious = []byte("previous-deadline-secret-0123456789abcdef")
	keyStranger = []byte("stranger-deadline-secret-0123456789abcdef")
)

const (
	org      = "6f1c2b8e-3d4a-4e5f-9a0b-1c2d3e4f5a6b"
	tenant   = org + ":live"
	recordID = "0b5f1e2a-7c3d-4e8f-9a1b-2c3d4e5f6a7b"
	deadline = "2026-10-05T12:00:00.123456Z"
)

// fakeAdmitter stands in for the database read: it records what the gate asked and
// answers as told. The real read is proved in admission_test.go.
type fakeAdmitter struct {
	mu       sync.Mutex
	calls    []Admission
	decision Decision
	err      error
	delay    time.Duration
}

func (f *fakeAdmitter) Admit(ctx context.Context, a Admission) (Decision, error) {
	f.mu.Lock()
	f.calls = append(f.calls, a)
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return Decision{}, ctx.Err()
		}
	}
	return f.decision, f.err
}

// fakeFencer stands in for the Redis write fence (internal/redis.Fencer, proved against a
// paused Redis in internal/services). It passes the context through, or fails.
type fakeFencer struct{ err error }

func (f fakeFencer) Fence(ctx context.Context, _ time.Time) (context.Context, error) {
	return ctx, f.err
}

type call struct {
	method  string
	target  string
	body    string
	headers map[string][]string
}

// signed builds a call carrying a signature by key over exactly what is sent.
func signed(key []byte, method, target, body, kind string) call {
	return call{method: method, target: target, body: body, headers: map[string][]string{
		HeaderDeadline:    {deadline},
		HeaderStartRecord: {recordID},
		HeaderCallKind:    {kind},
		HeaderSignature:   {Sign(key, method, target, []byte(body), recordID, kind, deadline)},
	}}
}

func (c call) with(name string, values ...string) call {
	h := map[string][]string{}
	for k, v := range c.headers {
		h[k] = v
	}
	if len(values) == 0 {
		delete(h, name)
	} else {
		h[name] = values
	}
	c.headers = h
	return c
}

type outcome struct {
	status   int
	reached  bool // the service behind the gate ran
	admitted bool // the database admission read ran
}

func run(t *testing.T, g *Gate, a *fakeAdmitter, c call) (outcome, *fakeAdmitter) {
	t.Helper()
	reached := false
	var seenBody []byte
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		seenBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(c.method, c.target, strings.NewReader(c.body))
	for k, vs := range c.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	g.Wrap(next).ServeHTTP(rec, req)
	if reached && string(seenBody) != c.body {
		t.Fatalf("the service saw body %q, want the exact signed body %q", seenBody, c.body)
	}
	return outcome{status: rec.Code, reached: reached, admitted: len(a.calls) > 0}, a
}

func newGate(t *testing.T, a *fakeAdmitter, previous string, timeout time.Duration) *Gate {
	t.Helper()
	g, err := New(string(keyCurrent), previous, timeout, a, fakeFencer{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

// A call whose writes cannot be fenced is refused before the service starts: without the
// fence a write could land after the cut-off.
func TestFenceFailureRefusesTheCall(t *testing.T) {
	a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: 10 * time.Second}}
	g, err := New(string(keyCurrent), "", 10*time.Second, a, fakeFencer{err: errors.New("redis: TIME timed out")})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := run(t, g, a, signed(keyCurrent, http.MethodPost, "/api/v1/publish", publishBody, "publish"))
	if want := (outcome{status: http.StatusServiceUnavailable, admitted: true}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

const publishBody = `{"tenant_id":"` + tenant + `","topic":"payout.sent","data":{"x":1}}`

func TestGateRules(t *testing.T) {
	destPath := "/api/v1/tenants/" + tenant + "/destinations"
	admitAll := Decision{Admit: true, RequestTimeout: 10 * time.Second}
	good := signed(keyCurrent, http.MethodPost, "/api/v1/publish", publishBody, "publish")
	refusedUnread := outcome{status: http.StatusForbidden}
	cases := []struct {
		name     string
		call     call
		previous string
		decision Decision
		err      error
		want     outcome
		tenant   string // the tenant the admission read must be asked about
	}{
		// Default deny: every non-GET without a signature, whatever its route.
		{name: "unsigned POST publish", call: call{method: "POST", target: "/api/v1/publish", body: publishBody}, want: refusedUnread},
		{name: "unsigned PUT tenant", call: call{method: "PUT", target: "/api/v1/tenants/" + tenant, body: "{}"}, want: refusedUnread},
		{name: "unsigned PATCH destination", call: call{method: "PATCH", target: destPath + "/des_1", body: `{"disabled_at":null}`}, want: refusedUnread},
		{name: "unsigned DELETE destination", call: call{method: "DELETE", target: destPath + "/des_1"}, want: refusedUnread},
		{name: "unsigned PUT disable", call: call{method: "PUT", target: destPath + "/des_1/disable"}, want: refusedUnread},
		{name: "unsigned POST retry", call: call{method: "POST", target: "/api/v1/retry", body: "{}"}, want: refusedUnread},
		{name: "unsigned POST unknown route", call: call{method: "POST", target: "/nope"}, want: refusedUnread},
		{name: "unsigned HEAD", call: call{method: "HEAD", target: destPath}, want: refusedUnread},
		{name: "unsigned OPTIONS", call: call{method: "OPTIONS", target: destPath}, want: refusedUnread},
		{name: "unsigned GET passes without a read", call: call{method: "GET", target: destPath}, want: outcome{status: 200, reached: true}},

		// A signed call is admitted only on the database's true answer.
		{name: "signed publish admitted", call: good, decision: admitAll, want: outcome{status: 200, reached: true, admitted: true}, tenant: tenant},
		{name: "signed GET job_list gets the read", call: signed(keyCurrent, "GET", destPath, "", "job_list"), decision: admitAll, want: outcome{status: 200, reached: true, admitted: true}, tenant: tenant},
		{name: "signed disable admitted", call: signed(keyCurrent, "PUT", destPath+"/des_1/disable", "", "job_disable"), decision: admitAll, want: outcome{status: 200, reached: true, admitted: true}, tenant: tenant},
		{name: "database says no", call: good, decision: Decision{Admit: false, RequestTimeout: time.Second}, want: outcome{status: http.StatusForbidden, admitted: true}, tenant: tenant},
		{name: "database unreachable", call: good, err: errors.New("dial tcp: connection refused"), want: outcome{status: http.StatusServiceUnavailable, admitted: true}, tenant: tenant},
		{name: "signature by the previous key during a rotation", call: signed(keyPrevious, "POST", "/api/v1/publish", publishBody, "publish"), previous: string(keyPrevious), decision: admitAll, want: outcome{status: 200, reached: true, admitted: true}, tenant: tenant},

		// A failed signature has no effect: no read, nothing reaches the service.
		{name: "previous key outside a rotation", call: signed(keyPrevious, "POST", "/api/v1/publish", publishBody, "publish"), decision: admitAll, want: refusedUnread},
		{name: "stranger key", call: signed(keyStranger, "POST", "/api/v1/publish", publishBody, "publish"), previous: string(keyPrevious), decision: admitAll, want: refusedUnread},
		{name: "body changed after signing", call: func() call { c := good; c.body = strings.Replace(publishBody, "x", "y", 1); return c }(), decision: admitAll, want: refusedUnread},
		{name: "body grown by one byte", call: func() call { c := good; c.body += " "; return c }(), decision: admitAll, want: refusedUnread},
		{name: "method changed", call: func() call {
			c := signed(keyCurrent, "PUT", destPath+"/des_1/enable", "", "job_enable")
			c.method = "DELETE"
			return c
		}(), decision: admitAll, want: refusedUnread},
		{name: "path changed to another tenant", call: func() call {
			c := signed(keyCurrent, "PUT", destPath+"/des_1/disable", "", "job_disable")
			c.target = "/api/v1/tenants/" + org + ":test/destinations/des_1/disable"
			return c
		}(), decision: admitAll, want: refusedUnread},
		{name: "query added after signing", call: func() call {
			c := signed(keyCurrent, "GET", destPath, "", "job_list")
			c.target += "?tenant_id=other"
			return c
		}(), decision: admitAll, want: refusedUnread},
		// The query is signed too. This query names no tenant, so only the signature can
		// catch it.
		{name: "query added to a signed publish, tenant untouched", call: func() call { c := good; c.target += "?x=1"; return c }(), decision: admitAll, want: refusedUnread},
		{name: "re-kinded", call: good.with(HeaderCallKind, "republish"), decision: admitAll, want: refusedUnread},
		{name: "re-dated", call: good.with(HeaderDeadline, "2026-10-05T12:00:00.123457Z"), decision: admitAll, want: refusedUnread},
		{name: "other start record", call: good.with(HeaderStartRecord, "0b5f1e2a-7c3d-4e8f-9a1b-2c3d4e5f6a7c"), decision: admitAll, want: refusedUnread},
		{name: "signature missing", call: good.with(HeaderSignature), decision: admitAll, want: refusedUnread},
		{name: "deadline missing", call: good.with(HeaderDeadline), decision: admitAll, want: refusedUnread},
		{name: "start record missing", call: good.with(HeaderStartRecord), decision: admitAll, want: refusedUnread},
		{name: "kind missing", call: good.with(HeaderCallKind), decision: admitAll, want: refusedUnread},
		{name: "signature without v1=", call: good.with(HeaderSignature, strings.TrimPrefix(good.headers[HeaderSignature][0], "v1=")), decision: admitAll, want: refusedUnread},
		{name: "signature upper-cased", call: good.with(HeaderSignature, "v1="+strings.ToUpper(strings.TrimPrefix(good.headers[HeaderSignature][0], "v1="))), decision: admitAll, want: refusedUnread},
		{name: "signature truncated", call: good.with(HeaderSignature, good.headers[HeaderSignature][0][:20]), decision: admitAll, want: refusedUnread},
		{name: "empty signature", call: good.with(HeaderSignature, ""), decision: admitAll, want: refusedUnread},
		{name: "two signatures", call: good.with(HeaderSignature, good.headers[HeaderSignature][0], good.headers[HeaderSignature][0]), decision: admitAll, want: refusedUnread},
		{name: "two deadlines", call: good.with(HeaderDeadline, deadline, deadline), decision: admitAll, want: refusedUnread},
		{name: "GET carrying only a signature header", call: call{method: "GET", target: destPath, headers: map[string][]string{HeaderSignature: {"v1=00"}}}, decision: admitAll, want: refusedUnread},

		// Validly signed but malformed: refused before any read.
		{name: "unknown kind", call: signed(keyCurrent, "POST", "/api/v1/publish", publishBody, "publish_all"), decision: admitAll, want: refusedUnread},
		{name: "kind in upper case", call: signed(keyCurrent, "POST", "/api/v1/publish", publishBody, "PUBLISH"), decision: admitAll, want: refusedUnread},
		{name: "deadline without microseconds", call: func() call {
			c := good
			d := "2026-10-05T12:00:00Z"
			c = c.with(HeaderDeadline, d).with(HeaderSignature, Sign(keyCurrent, c.method, c.target, []byte(c.body), recordID, "publish", d))
			return c
		}(), decision: admitAll, want: refusedUnread},
		{name: "deadline with an offset", call: func() call {
			c := good
			d := "2026-10-05T21:00:00.123456+09:00"
			c = c.with(HeaderDeadline, d).with(HeaderSignature, Sign(keyCurrent, c.method, c.target, []byte(c.body), recordID, "publish", d))
			return c
		}(), decision: admitAll, want: refusedUnread},
		{name: "start record not a uuid", call: func() call {
			c := good
			id := "1 OR true"
			c = c.with(HeaderStartRecord, id).with(HeaderSignature, Sign(keyCurrent, c.method, c.target, []byte(c.body), id, "publish", deadline))
			return c
		}(), decision: admitAll, want: refusedUnread},

		// The tenant the call acts on must be one the gate can name and compare.
		{name: "publish without tenant_id", call: signed(keyCurrent, "POST", "/api/v1/publish", `{"topic":"t"}`, "publish"), decision: admitAll, want: refusedUnread},
		{name: "publish with an empty tenant_id", call: signed(keyCurrent, "POST", "/api/v1/publish", `{"tenant_id":"","topic":"t"}`, "publish"), decision: admitAll, want: refusedUnread},
		{name: "publish body not JSON", call: signed(keyCurrent, "POST", "/api/v1/publish", "tenant_id="+tenant, "publish"), decision: admitAll, want: refusedUnread},
		{name: "publish tenant_id not a string", call: signed(keyCurrent, "POST", "/api/v1/publish", `{"tenant_id":7}`, "publish"), decision: admitAll, want: refusedUnread},
		{name: "publish with a case-folded second tenant_id", call: signed(keyCurrent, "POST", "/api/v1/publish", `{"tenant_id":"`+tenant+`","TENANT_ID":"`+org+`:test"}`, "publish"), decision: admitAll, want: outcome{status: 200, reached: true, admitted: true}, tenant: org + ":test"},
		{name: "path tenant with a different body tenant_id", call: signed(keyCurrent, "PUT", "/api/v1/tenants/"+tenant, `{"tenant_id":"`+org+`:test"}`, "proxied_mutation"), decision: admitAll, want: refusedUnread},
		{name: "path tenant with a different query tenant_id", call: signed(keyCurrent, "GET", destPath+"?tenant_id[0]="+org+":test", "", "job_list"), decision: admitAll, want: refusedUnread},
		{name: "retry names no tenant", call: signed(keyCurrent, "POST", "/api/v1/retry", `{"event_id":"e","destination_id":"des_1"}`, "proxied_mutation"), decision: admitAll, want: refusedUnread},
		{name: "empty tenant segment", call: signed(keyCurrent, "PUT", "/api/v1/tenants//destinations/des_1/disable", "", "job_disable"), decision: admitAll, want: refusedUnread},
		{name: "publish with a trailing slash", call: signed(keyCurrent, "POST", "/api/v1/publish/", publishBody, "publish"), decision: admitAll, want: refusedUnread},
		{name: "signed call to an unknown route", call: signed(keyCurrent, "POST", "/api/v1/tenants", "{}", "proxied_mutation"), decision: admitAll, want: refusedUnread},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &fakeAdmitter{decision: tc.decision, err: tc.err}
			got, a := run(t, newGate(t, a, tc.previous, 10*time.Second), a, tc.call)
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if tc.tenant != "" {
				want := Admission{StartRecordID: tc.call.headers[HeaderStartRecord][0], Kind: tc.call.headers[HeaderCallKind][0], Deadline: tc.call.headers[HeaderDeadline][0], Tenant: tc.tenant}
				if len(a.calls) != 1 || a.calls[0] != want {
					t.Fatalf("admission read asked %+v, want exactly %+v", a.calls, want)
				}
			}
		})
	}
}

func TestBodyOverLimitIsRefused(t *testing.T) {
	body := `{"tenant_id":"` + tenant + `","data":"` + strings.Repeat("a", MaxBodyBytes) + `"}`
	a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: time.Second}}
	got, _ := run(t, newGate(t, a, "", 10*time.Second), a, signed(keyCurrent, "POST", "/api/v1/publish", body, "publish"))
	if got.reached || got.admitted || got.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %+v, want 413 with no read and nothing reached", got)
	}
}

// Each route a signed call may take admits only the call kinds MB Wallet sends on it
// (spec "Deadline gate"); every other pairing is refused before the read, so a start
// record of one kind, admitted by the database for that kind, cannot carry another call.
func TestSignedKindMustFitTheRoute(t *testing.T) {
	kinds := []string{"publish", "redrive", "republish", "proxied_mutation", "job_disable", "job_list", "job_enable"}
	tenantPath := "/api/v1/tenants/" + tenant
	destPath := tenantPath + "/destinations"
	routes := []struct {
		method, target, body string
		allowed              []string
	}{
		{"POST", "/api/v1/publish", publishBody, []string{"publish", "redrive", "republish"}},
		{"PUT", tenantPath, "", []string{"proxied_mutation"}},
		{"DELETE", tenantPath, "", []string{"proxied_mutation"}},
		{"GET", destPath, "", []string{"job_list"}},
		{"POST", destPath, `{"type":"webhook","topics":["*"]}`, []string{"proxied_mutation"}},
		{"PATCH", destPath + "/des_1", `{"topics":["*"]}`, []string{"proxied_mutation"}},
		{"DELETE", destPath + "/des_1", "", []string{"proxied_mutation"}},
		{"PUT", destPath + "/des_1/enable", "", []string{"job_enable", "proxied_mutation"}},
		{"PUT", destPath + "/des_1/disable", "", []string{"job_disable", "proxied_mutation"}},
		// Signed calls on routes MB Wallet never signs for: refused whatever the kind.
		{"POST", "/api/v1/retry", `{"event_id":"e","destination_id":"des_1"}`, nil},
		{"GET", tenantPath, "", nil},
		{"GET", destPath + "/des_1", "", nil},
		{"GET", tenantPath + "/token", "", nil},
		{"GET", tenantPath + "/portal", "", nil},
		{"PUT", destPath + "/des_1/enable/x", "", nil},
		{"PUT", destPath + "//disable", "", nil},
	}
	for _, rt := range routes {
		for _, kind := range kinds {
			allowed := slices.Contains(rt.allowed, kind)
			t.Run(rt.method+" "+rt.target+" as "+kind, func(t *testing.T) {
				a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: 10 * time.Second}}
				got, a := run(t, newGate(t, a, "", 10*time.Second), a, signed(keyCurrent, rt.method, rt.target, rt.body, kind))
				want := outcome{status: http.StatusForbidden}
				if allowed {
					want = outcome{status: http.StatusOK, reached: true, admitted: true}
				}
				if got != want {
					t.Fatalf("got %+v, want %+v", got, want)
				}
				if allowed && (len(a.calls) != 1 || a.calls[0].Kind != kind || a.calls[0].Tenant != tenant) {
					t.Fatalf("admission read asked %+v, want kind %s for tenant %s", a.calls, kind, tenant)
				}
			})
		}
	}
}

// cutOff runs one admitted call whose service never finishes on its own and reports
// how long after the gate's start the caller got its answer, and whether the service
// saw its context end.
func cutOff(t *testing.T, configured, stored, readDelay time.Duration) (time.Duration, int, bool) {
	t.Helper()
	a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: stored}, delay: readDelay}
	g := newGate(t, a, "", configured)
	cancelled := make(chan bool, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cancelled <- true
		case <-time.After(5 * time.Second):
			cancelled <- false
			w.WriteHeader(http.StatusOK)
		}
	})
	c := signed(keyCurrent, "POST", "/api/v1/publish", publishBody, "publish")
	req := httptest.NewRequest(c.method, c.target, bytes.NewReader([]byte(c.body)))
	for k, vs := range c.headers {
		req.Header[k] = vs
	}
	rec := httptest.NewRecorder()
	start := time.Now()
	g.Wrap(next).ServeHTTP(rec, req)
	took := time.Since(start)
	select {
	case v := <-cancelled:
		return took, rec.Code, v
	case <-time.After(6 * time.Second):
		return took, rec.Code, false
	}
}

func TestAdmittedCallIsCutOffAtTheSmallerTimeout(t *testing.T) {
	cases := []struct {
		name                         string
		configured, stored, readWait time.Duration
		want                         time.Duration
	}{
		{name: "stored is smaller", configured: 2 * time.Second, stored: 300 * time.Millisecond, want: 300 * time.Millisecond},
		{name: "configured is smaller", configured: 300 * time.Millisecond, stored: 2 * time.Second, want: 300 * time.Millisecond},
		{name: "measured from the timer's start, the read included", configured: 2 * time.Second, stored: 400 * time.Millisecond, readWait: 250 * time.Millisecond, want: 400 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			took, code, cancelled := cutOff(t, tc.configured, tc.stored, tc.readWait)
			if code == http.StatusOK || !cancelled {
				t.Fatalf("status %d, service context ended %v: the call was not cut off", code, cancelled)
			}
			if took < tc.want-20*time.Millisecond || took > tc.want+150*time.Millisecond {
				t.Fatalf("cut off after %v, want about %v", took, tc.want)
			}
		})
	}
}

// A call whose stored timeout was spent by the time the read answered must not start the
// service at all. The service runs on its own goroutine under the cut-off, so the test
// waits for a start that could come after the gate has answered.
func TestReadThatOutlastsTheTimeoutStartsNothing(t *testing.T) {
	const runs = 20
	started := make(chan struct{}, runs)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		w.WriteHeader(http.StatusOK)
	})
	for i := 0; i < runs; i++ {
		a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: 20 * time.Millisecond}, delay: 40 * time.Millisecond}
		c := signed(keyCurrent, "POST", "/api/v1/publish", publishBody, "publish")
		req := httptest.NewRequest(c.method, c.target, strings.NewReader(c.body))
		for k, vs := range c.headers {
			req.Header[k] = vs
		}
		rec := httptest.NewRecorder()
		newGate(t, a, "", 10*time.Second).Wrap(next).ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("run %d: status %d, want 503", i, rec.Code)
		}
	}
	select {
	case <-started:
		t.Fatal("the service started for a call whose request timeout was spent before admission")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestNewRefusesAGateThatCouldNotDeny(t *testing.T) {
	a := &fakeAdmitter{}
	cases := []struct {
		name              string
		current, previous string
		timeout           time.Duration
		admitter          Admitter
		noFence           bool
	}{
		{name: "no secret", current: "", timeout: time.Second, admitter: a},
		{name: "short secret", current: "short", timeout: time.Second, admitter: a},
		{name: "short previous secret", current: string(keyCurrent), previous: "short", timeout: time.Second, admitter: a},
		{name: "previous equals current", current: string(keyCurrent), previous: string(keyCurrent), timeout: time.Second, admitter: a},
		{name: "no timeout", current: string(keyCurrent), timeout: 0, admitter: a},
		{name: "no database", current: string(keyCurrent), timeout: time.Second, admitter: nil},
		{name: "no write fence", current: string(keyCurrent), timeout: time.Second, admitter: a, noFence: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fencer Fencer = fakeFencer{}
			if tc.noFence {
				fencer = nil
			}
			if _, err := New(tc.current, tc.previous, tc.timeout, tc.admitter, fencer); err == nil {
				t.Fatal("New accepted a configuration that cannot enforce the gate")
			}
		})
	}
}

// A database that never answers must not hold the call open: the read is bounded by the
// request timeout and the caller is refused within it.
func TestHungDatabaseIsRefusedWithinTheTimeout(t *testing.T) {
	a := &fakeAdmitter{decision: Decision{Admit: true, RequestTimeout: time.Hour}, delay: time.Hour}
	g := newGate(t, a, "", 200*time.Millisecond)
	done := make(chan outcome, 1)
	go func() {
		got, _ := run(t, g, a, signed(keyCurrent, "POST", "/api/v1/publish", publishBody, "publish"))
		done <- got
	}()
	select {
	case got := <-done:
		if got.reached || got.status != http.StatusServiceUnavailable {
			t.Fatalf("got %+v: a hung admission read must refuse", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no answer after 2s: a hung admission read held the call past the 200ms timeout")
	}
}
