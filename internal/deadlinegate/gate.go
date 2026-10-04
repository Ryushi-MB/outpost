// Package deadlinegate is MB Wallet's deadline gate in front of Outpost's API (Ospec
// add-agency-api-access tasks 9.9, 9.14 and 9.18).
//
// Every non-GET request must carry a valid Mb-Deadline-Sig, whatever its route. A call
// whose signature fails is refused before anything else happens. Every signed call, a
// signed GET included, is admitted only on a true answer from one read of MB Wallet's
// primary database (see admission.go), and an admitted call is cut off at the smaller of
// the gate's configured request timeout and the one stored on its start record, both
// measured from the gate's own monotonic timer. The gate never reads its host's clock to
// decide anything: whether D has passed is the database's answer.
package deadlinegate

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	HeaderDeadline    = "Mb-Deadline"
	HeaderSignature   = "Mb-Deadline-Sig"
	HeaderStartRecord = "Mb-Start-Record"
	HeaderCallKind    = "Mb-Call-Kind"
	// DeadlineLayout is the one accepted spelling of D: UTC, microseconds, as Postgres
	// stores a timestamptz.
	DeadlineLayout = "2006-01-02T15:04:05.000000Z"
	// MaxBodyBytes bounds the body the gate reads to hash it.
	MaxBodyBytes = 8 << 20
	// minSecretBytes is the shortest deadline signing secret the gate accepts.
	minSecretBytes = 32
)

// The call kinds of the spec; anything else is refused before the database read.
var callKinds = map[string]bool{
	"publish": true, "redrive": true, "republish": true, "proxied_mutation": true,
	"job_disable": true, "job_list": true, "job_enable": true,
}

var startRecordID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Admission is what one signed call asks the database.
type Admission struct {
	StartRecordID string
	Kind          string
	Deadline      string
	// Tenant is the Outpost tenant the call acts on, "<agency>:<mode>".
	Tenant string
}

// Decision is the database's answer: admit or not, and the request timeout stored on
// the start record.
type Decision struct {
	Admit          bool
	RequestTimeout time.Duration
}

// Admitter runs the admission read. An error refuses the call.
type Admitter interface {
	Admit(ctx context.Context, a Admission) (Decision, error)
}

type Gate struct {
	keys           [][]byte
	requestTimeout time.Duration
	admitter       Admitter
}

// New builds a gate. It refuses any configuration under which the gate could not deny:
// no or a short secret, a previous secret equal to the current one, no request timeout,
// or no database.
func New(current, previous string, requestTimeout time.Duration, admitter Admitter) (*Gate, error) {
	if len(current) < minSecretBytes {
		return nil, fmt.Errorf("deadline gate: WEBHOOK_DEADLINE_SECRET must be at least %d bytes", minSecretBytes)
	}
	keys := [][]byte{[]byte(current)}
	if previous != "" {
		if len(previous) < minSecretBytes {
			return nil, fmt.Errorf("deadline gate: WEBHOOK_DEADLINE_SECRET_PREVIOUS must be at least %d bytes", minSecretBytes)
		}
		if previous == current {
			return nil, errors.New("deadline gate: WEBHOOK_DEADLINE_SECRET_PREVIOUS equals the current secret")
		}
		keys = append(keys, []byte(previous))
	}
	if requestTimeout <= 0 {
		return nil, errors.New("deadline gate: the webhook service request timeout must be set")
	}
	if admitter == nil {
		return nil, errors.New("deadline gate: the gate database must be set")
	}
	return &Gate{keys: keys, requestTimeout: requestTimeout, admitter: admitter}, nil
}

// Sign returns the Mb-Deadline-Sig value for a call: "v1=" and the hex HMAC-SHA256, keyed
// by the deadline signing secret, of
//
//	"mb-deadline-v1\n" method "\n" target "\n" hex(sha256(body)) "\n" startRecordID "\n" kind "\n" deadline
//
// where target is the request target exactly as sent (path and, if any, "?" and query).
func Sign(key []byte, method, target string, body []byte, startRecordID, kind, deadline string) string {
	sum := sha256.Sum256(body)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(strings.Join([]string{"mb-deadline-v1", method, target, hex.EncodeToString(sum[:]), startRecordID, kind, deadline}, "\n")))
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

func (g *Gate) verify(method, target string, body []byte, h signedHeaders) bool {
	got := []byte(h.signature)
	ok := false
	for _, key := range g.keys {
		want := []byte(Sign(key, method, target, body, h.startRecord, h.kind, h.deadline))
		// Every key is compared, so the time taken does not say which one matched.
		if hmac.Equal(got, want) {
			ok = true
		}
	}
	return ok
}

type signedHeaders struct{ deadline, signature, startRecord, kind string }

var gateHeaders = []string{HeaderDeadline, HeaderSignature, HeaderStartRecord, HeaderCallKind}

// readHeaders reports whether the call claims to be signed, and its four headers when
// each is present exactly once.
func readHeaders(h http.Header) (claimed bool, out signedHeaders, ok bool) {
	vals := make([]string, len(gateHeaders))
	ok = true
	for i, name := range gateHeaders {
		vs := h.Values(name)
		if len(vs) > 0 {
			claimed = true
		}
		if len(vs) != 1 {
			ok = false
			continue
		}
		vals[i] = vs[0]
	}
	return claimed, signedHeaders{deadline: vals[0], signature: vals[1], startRecord: vals[2], kind: vals[3]}, ok
}

func canonicalDeadline(s string) bool {
	t, err := time.Parse(DeadlineLayout, s)
	return err == nil && t.UTC().Format(DeadlineLayout) == s
}

// tenantOf names the Outpost tenant a call acts on, read the way Outpost's router and
// handlers read it, or reports that the call names none the gate can compare.
func tenantOf(r *http.Request, body []byte) (string, bool) {
	var tenant string
	switch rest, isTenantRoute := strings.CutPrefix(r.URL.Path, "/api/v1/tenants/"); {
	case isTenantRoute:
		tenant, _, _ = strings.Cut(rest, "/")
	case r.URL.Path == "/api/v1/publish":
		t, ok := bodyTenant(body)
		if !ok || t == nil {
			return "", false
		}
		tenant = *t
	default:
		// /retry names a destination and no tenant; every other route names none either.
		return "", false
	}
	if tenant == "" {
		return "", false
	}
	// A body or query that names a tenant must name the same one.
	if len(body) > 0 {
		if t, ok := bodyTenant(body); ok && t != nil && *t != tenant {
			return "", false
		}
	}
	for key, values := range r.URL.Query() {
		if key == "tenant_id" || strings.HasPrefix(key, "tenant_id[") {
			for _, v := range values {
				if v != tenant {
					return "", false
				}
			}
		}
	}
	return tenant, true
}

// bodyTenant decodes tenant_id with encoding/json into a struct, as Outpost's handlers
// bind it (case-insensitive keys, the last one wins), so the gate compares the tenant
// Outpost will act on. ok is false when the body is not a JSON object.
func bodyTenant(body []byte) (*string, bool) {
	var v struct {
		TenantID *string `json:"tenant_id"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, false
	}
	return v.TenantID, true
}

func refuse(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": status, "message": "deadline gate: " + reason})
}

const cutOffBody = `{"code":503,"message":"deadline gate: call cut off at its request timeout"}`

// Wrap puts the gate in front of next, the whole of Outpost's API.
func (g *Gate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // carries the monotonic reading; only durations are taken from it
		claimed, h, complete := readHeaders(r.Header)
		if !claimed {
			if r.Method == http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}
			refuse(w, http.StatusForbidden, "unsigned call")
			return
		}
		if !complete {
			refuse(w, http.StatusForbidden, "signature headers missing or repeated")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
		if err != nil {
			refuse(w, http.StatusBadRequest, "body unreadable")
			return
		}
		if len(body) > MaxBodyBytes {
			refuse(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		if !g.verify(r.Method, r.RequestURI, body, h) {
			refuse(w, http.StatusForbidden, "signature does not verify")
			return
		}
		if !callKinds[h.kind] || !canonicalDeadline(h.deadline) || !startRecordID.MatchString(h.startRecord) {
			refuse(w, http.StatusForbidden, "malformed call kind, deadline or start record")
			return
		}
		tenant, ok := tenantOf(r, body)
		if !ok {
			refuse(w, http.StatusForbidden, "call names no tenant the gate can compare")
			return
		}

		readCtx, cancelRead := context.WithTimeout(r.Context(), g.requestTimeout-time.Since(start))
		d, err := g.admitter.Admit(readCtx, Admission{StartRecordID: h.startRecord, Kind: h.kind, Deadline: h.deadline, Tenant: tenant})
		cancelRead()
		if err != nil {
			refuse(w, http.StatusServiceUnavailable, "admission read failed")
			return
		}
		if !d.Admit {
			refuse(w, http.StatusForbidden, "not admitted")
			return
		}
		limit := min(g.requestTimeout, d.RequestTimeout)
		remaining := limit - time.Since(start)
		if remaining <= 0 {
			refuse(w, http.StatusServiceUnavailable, "request timeout spent before admission")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		http.TimeoutHandler(next, remaining, cutOffBody).ServeHTTP(w, r)
	})
}
