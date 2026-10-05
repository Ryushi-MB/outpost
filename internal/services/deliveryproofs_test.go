package services_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startRecord writes a start record as MB Wallet writes it: for the agency's tenant of mode
// and, for a job call, under the access state and cycle marker given, with the stop job
// holding its lease for the next hour.
func (a *gatedAPI) startRecord(t *testing.T, kind, mode, accessState, marker string) gatedRecord {
	t.Helper()
	ctx := context.Background()
	var lease, state, cycle any
	if strings.HasPrefix(kind, "job_") || kind == "publish" || kind == "republish" {
		var token int64
		require.NoError(t, a.admin.QueryRow(ctx, `UPDATE stop_job_lease SET fencing_token = fencing_token + 1,
			holder = 'services-test', expires_at = clock_timestamp() + interval '1 hour' RETURNING fencing_token`).Scan(&token))
		lease = token
	}
	if strings.HasPrefix(kind, "job_") {
		state, cycle = accessState, marker
	}
	var r gatedRecord
	require.NoError(t, a.admin.QueryRow(ctx, `
		WITH t AS (SELECT date_trunc('microseconds', clock_timestamp()) AS created)
		INSERT INTO outbound_start_records (org_id, mode, kind, created_at, deadline, lease_token, access_state, cycle_marker,
		  request_timeout_ms, transaction_timeout_ms, proxy_call_timeout_ms)
		SELECT $1, $2, $3, t.created, t.created + interval '1 hour', $4, $5, $6::uuid, 10000, 5000, 3600000 FROM t
		RETURNING id::text, to_char(deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
		a.agency, mode, kind, lease, state, cycle).Scan(&r.id, &r.deadline))
	t.Cleanup(func() {
		_, _ = a.admin.Exec(context.Background(), "DELETE FROM outbound_start_records WHERE id = $1::uuid", r.id)
	})
	return r
}

// setAccess moves the agency's API access, as the access switch does.
func (a *gatedAPI) setAccess(t *testing.T, state, marker string) {
	t.Helper()
	var cycle any
	if marker != "" {
		cycle = marker
	}
	_, err := a.admin.Exec(context.Background(), `INSERT INTO agency_api_access (org_id, state, cycle_marker) VALUES ($1, $2, $3::uuid)
		ON CONFLICT (org_id) DO UPDATE SET state = EXCLUDED.state, cycle_marker = EXCLUDED.cycle_marker`, a.agency, state, cycle)
	require.NoError(t, err)
}

// hits is a webhook receiver that records every request's headers by the event marker in
// its body, and answers each with status(marker, attempt).
type hits struct {
	mu   sync.Mutex
	seen map[string][]http.Header
	srv  *httptest.Server
}

func newHits(t *testing.T, status func(marker string, attempt int) int) *hits {
	h := &hits{seen: map[string][]http.Header{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		marker := ""
		for m := range h.markers(string(b)) {
			marker = m
		}
		h.mu.Lock()
		h.seen[marker] = append(h.seen[marker], r.Header.Clone())
		n := len(h.seen[marker])
		h.mu.Unlock()
		w.WriteHeader(status(marker, n))
	}))
	t.Cleanup(h.srv.Close)
	return h
}

// markers yields the "mk-..." marker a test event carries in its body.
func (h *hits) markers(body string) map[string]bool {
	out := map[string]bool{}
	if i := strings.Index(body, "mk-"); i >= 0 {
		j := i
		for j < len(body) && body[j] != '"' {
			j++
		}
		out[body[i:j]] = true
	}
	return out
}

func (h *hits) of(marker string) []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]http.Header(nil), h.seen[marker]...)
}

// Ospec add-agency-api-access task 9.11: disabling an agency's destinations, in its live
// and its test tenant, stops the retries of events the service already accepted. Each
// event's first attempt fails; Stopping then disables the destination through the gate
// (a signed job_disable); no retry may reach it. A third destination left enabled gets
// its first retry (30 s) in the same window, so the window covers a retry.
func TestDisablingADestinationStopsAcceptedRetries(t *testing.T) {
	receiver := newHits(t, func(string, int) int { return http.StatusInternalServerError })
	api := startGatedAPI(t, 5*time.Second, true)
	const markerA = "33333333-3333-4333-8333-333333333333"

	type dest struct{ tenant, id, marker string }
	var stopped []dest
	var control dest
	for _, mode := range []string{"live", "test"} {
		tenant := api.agency + ":" + mode
		mutation := api.startRecord(t, "proxied_mutation", mode, "", "")
		status, body := api.signedWith(t, mutation, "proxied_mutation", http.MethodPut, "/api/v1/tenants/"+tenant, map[string]any{})
		require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status, body["_raw"])
		names := []string{"stopped"}
		if mode == "live" {
			names = append(names, "control")
		}
		for _, name := range names {
			marker := fmt.Sprintf("mk-%s-%s-%d", mode, name, time.Now().UnixNano())
			status, body = api.signedWith(t, mutation, "proxied_mutation", http.MethodPost, "/api/v1/tenants/"+tenant+"/destinations", map[string]any{
				"type": "webhook", "topics": []string{"user.created"},
				"config": map[string]any{"url": receiver.srv.URL + "/" + name}, "filter": map[string]any{"data": map[string]any{"marker": marker}},
			})
			require.Equal(t, http.StatusCreated, status, body["_raw"])
			d := dest{tenant: tenant, id: body["id"].(string), marker: marker}
			if name == "control" {
				control = d
			} else {
				stopped = append(stopped, d)
			}
		}
	}
	for _, d := range append([]dest{control}, stopped...) {
		mode := strings.SplitN(d.tenant, ":", 2)[1]
		pub := api.startRecord(t, "publish", mode, "", "")
		status, body := api.signedWith(t, pub, "publish", http.MethodPost, "/api/v1/publish", map[string]any{
			"id": d.marker, "tenant_id": d.tenant, "topic": "user.created", "eligible_for_retry": true,
			"data": map[string]any{"marker": d.marker},
		})
		require.Equal(t, http.StatusAccepted, status, body["_raw"])
	}
	for _, d := range append([]dest{control}, stopped...) {
		require.Eventually(t, func() bool { return len(receiver.of(d.marker)) == 1 }, 20*time.Second, 100*time.Millisecond,
			"first attempt of %s never arrived", d.marker)
	}

	// Access switched off: Stopping disables the agency's destinations through the gate.
	api.setAccess(t, "stopping", markerA)
	for _, d := range stopped {
		mode := strings.SplitN(d.tenant, ":", 2)[1]
		job := api.startRecord(t, "job_disable", mode, "stopping", markerA)
		status, body := api.signedWith(t, job, "job_disable", http.MethodPut, "/api/v1/tenants/"+d.tenant+"/destinations/"+d.id+"/disable", nil)
		require.Equal(t, http.StatusOK, status, body["_raw"])
	}

	require.Eventually(t, func() bool { return len(receiver.of(control.marker)) >= 2 }, 60*time.Second, 200*time.Millisecond,
		"the enabled control destination never got its 30 s retry, so the window proves nothing")
	time.Sleep(5 * time.Second)
	for _, d := range stopped {
		require.Len(t, receiver.of(d.marker), 1, "%s: a retry reached a disabled destination", d.tenant)
	}
}

// Ospec add-agency-api-access task 9.12: MB Wallet publishes the body event id as the
// metadata key Mb-Event-Id, and every attempt, the first and each retry, carries it as the
// HTTP header Mb-Event-Id with the same value, though Outpost's own event id (the publish
// id) differs from it, as a redrive's does.
func TestEveryAttemptCarriesMbEventID(t *testing.T) {
	receiver := newHits(t, func(_ string, attempt int) int {
		if attempt == 1 {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	})
	api := startGatedAPI(t, 5*time.Second, true)
	base := "/api/v1/tenants/" + api.tenant
	status, body := api.signed(t, "proxied_mutation", http.MethodPut, base, map[string]any{})
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status, body["_raw"])
	marker := fmt.Sprintf("mk-event-id-%d", time.Now().UnixNano())
	status, body = api.signed(t, "proxied_mutation", http.MethodPost, base+"/destinations", map[string]any{
		"type": "webhook", "topics": []string{"user.created"}, "config": map[string]any{"url": receiver.srv.URL + "/hook"},
	})
	require.Equal(t, http.StatusCreated, status, body["_raw"])

	bodyID := "MB-EVENT-" + strings.ToUpper(marker[len("mk-event-id-"):])
	status, body = api.signed(t, "publish", http.MethodPost, "/api/v1/publish", map[string]any{
		"id": "publish-" + marker, "tenant_id": api.tenant, "topic": "user.created", "eligible_for_retry": true,
		"metadata": map[string]string{"Mb-Event-Id": bodyID},
		"data":     map[string]any{"id": bodyID, "marker": marker},
	})
	require.Equal(t, http.StatusAccepted, status, body["_raw"])
	require.Eventually(t, func() bool { return len(receiver.of(marker)) >= 2 }, 60*time.Second, 200*time.Millisecond,
		"the failed first attempt was not retried")
	attempts := receiver.of(marker)
	for i, h := range attempts[:2] {
		require.Equal(t, []string{bodyID}, h.Values("Mb-Event-Id"), "attempt %d: Mb-Event-Id (headers %v)", i+1, h)
	}
}
