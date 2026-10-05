package services_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hookdeck/outpost/internal/config"
	"github.com/hookdeck/outpost/internal/deadlinegate"
	"github.com/hookdeck/outpost/internal/logging"
	"github.com/hookdeck/outpost/internal/services"
	"github.com/hookdeck/outpost/internal/telemetry"
	"github.com/hookdeck/outpost/internal/util/testinfra"
	"github.com/hookdeck/outpost/internal/util/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const fenceSecret = "deadline-gate-fence-secret-0123456789abcdef"

// gatedAPI is the built API service behind the deadline gate, with a real Redis Stack, and
// an admitted start record per call kind for the second agency of MB Wallet's schema.
type gatedAPI struct {
	addr    string
	tenant  string
	records map[string]gatedRecord
	link    *stallingLink
}

type gatedRecord struct{ id, deadline string }

// startGatedAPI needs Docker (Redis Stack, RabbitMQ, Postgres via testinfra) and
// MB_GATE_TEST_ADMIN_URL / MB_GATE_TEST_GATE_URL (MB Wallet's schema, see
// internal/deadlinegate/admission_test.go). It uses the second agency org so it does not
// race the admission tests, which use the first.
func startGatedAPI(t *testing.T, requestTimeout time.Duration) *gatedAPI {
	t.Helper()
	testutil.CheckIntegrationTest(t)
	adminURL, gateURL := os.Getenv("MB_GATE_TEST_ADMIN_URL"), os.Getenv("MB_GATE_TEST_GATE_URL")
	if adminURL == "" || gateURL == "" {
		t.Skip("MB_GATE_TEST_ADMIN_URL and MB_GATE_TEST_GATE_URL are unset: the gate needs MB Wallet's schema")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	var agency string
	require.NoError(t, admin.QueryRow(ctx, "SELECT id::text FROM orgs WHERE kind = 'agency' ORDER BY created_at, id OFFSET 1 LIMIT 1").Scan(&agency))
	var prevState *string
	_ = admin.QueryRow(ctx, "SELECT state FROM agency_api_access WHERE org_id = $1", agency).Scan(&prevState)
	_, err = admin.Exec(ctx, `INSERT INTO agency_api_access (org_id, state) VALUES ($1, 'on')
		ON CONFLICT (org_id) DO UPDATE SET state = 'on', cycle_marker = NULL`, agency)
	require.NoError(t, err)
	var token int64
	require.NoError(t, admin.QueryRow(ctx, "SELECT fencing_token FROM stop_job_lease").Scan(&token))

	api := &gatedAPI{tenant: agency + ":live", records: map[string]gatedRecord{}}
	var made []string
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DELETE FROM outbound_start_records WHERE id = ANY($1::uuid[])", made)
		if prevState == nil {
			_, _ = admin.Exec(context.Background(), "DELETE FROM agency_api_access WHERE org_id = $1", agency)
		} else {
			_, _ = admin.Exec(context.Background(), "UPDATE agency_api_access SET state = $2 WHERE org_id = $1", agency, *prevState)
		}
	})
	for _, kind := range []string{"proxied_mutation", "publish"} {
		var lease any
		if kind == "publish" {
			lease = token
		}
		var r gatedRecord
		require.NoError(t, admin.QueryRow(ctx, `
			INSERT INTO outbound_start_records (org_id, mode, kind, created_at, deadline, lease_token, request_timeout_ms)
			VALUES ($1, 'live', $2, clock_timestamp(), date_trunc('microseconds', clock_timestamp() + interval '1 hour'), $3, $4)
			RETURNING id::text, to_char(deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
			agency, kind, lease, requestTimeout.Milliseconds()).Scan(&r.id, &r.deadline))
		made = append(made, r.id)
		api.records[kind] = r
	}

	redisConfig := testinfra.NewRedisStackConfig(t)
	c := &config.Config{}
	c.InitDefaults()
	c.Service = config.ServiceTypeAPI.String()
	c.APIPort = testutil.RandomPortNumber()
	c.APIKey = "apikey"
	c.APIJWTSecret = "jwtsecret"
	c.AESEncryptionSecret = "encryptionsecret"
	c.Topics = testutil.TestTopics
	c.Telemetry.Disabled = true
	api.link = newStallingLink(t, fmt.Sprintf("%s:%d", redisConfig.Host, redisConfig.Port))
	c.Redis.Host, c.Redis.Port = "127.0.0.1", api.link.port
	c.MQs.RabbitMQ.ServerURL = testinfra.EnsureRabbitMQ()
	c.PostgresURL = testinfra.NewPostgresConfig(t)
	c.DeadlineGate.Secret = fenceSecret
	c.DeadlineGate.DatabaseURL = gateURL
	c.DeadlineGate.LocalPlaintext = true
	c.DeadlineGate.RequestTimeoutMs = int(requestTimeout.Milliseconds())
	require.NoError(t, c.Validate(config.Flags{}))

	svcCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := logging.NewLogger(logging.WithLogLevel("error"))
	require.NoError(t, err)
	builder := services.NewServiceBuilder(svcCtx, c, logger, telemetry.New(logger, c.Telemetry.ToTelemetryConfig(), ""))
	supervisor, err := builder.BuildWorkers()
	require.NoError(t, err)
	t.Cleanup(func() { builder.Cleanup(context.Background()) })
	go func() { _ = supervisor.Run(svcCtx) }()

	api.addr = fmt.Sprintf("http://127.0.0.1:%d", c.APIPort)
	require.Eventually(t, func() bool {
		res, err := http.Get(api.addr + "/healthz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)
	return api
}

// signed sends a call signed for the start record of kind, as MB Wallet's Worker does.
func (a *gatedAPI) signed(t *testing.T, kind, method, target string, body any) (int, map[string]any) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	rec := a.records[kind]
	req, err := http.NewRequest(method, a.addr+target, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer apikey")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(deadlinegate.HeaderDeadline, rec.deadline)
	req.Header.Set(deadlinegate.HeaderStartRecord, rec.id)
	req.Header.Set(deadlinegate.HeaderCallKind, kind)
	req.Header.Set(deadlinegate.HeaderSignature, deadlinegate.Sign([]byte(fenceSecret), method, target, raw, rec.id, kind, rec.deadline))
	return do(t, req)
}

func (a *gatedAPI) get(t *testing.T, target string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, a.addr+target, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer apikey")
	return do(t, req)
}

func do(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{"_raw": string(raw)}
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

// stallingLink is the network between the service and Redis. Once stalled, it holds the
// first write a connection sends (a MULTI, EVAL or HSET) and everything after it until
// released, then delivers it to Redis even if the service has hung up meanwhile, as a
// stalled network does. Reads, TIME included, pass until a write is held.
type stallingLink struct {
	port     int
	upstream string
	stalled  atomic.Bool
	released chan struct{}
	once     sync.Once
}

func newStallingLink(t *testing.T, upstream string) *stallingLink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := &stallingLink{port: ln.Addr().(*net.TCPAddr).Port, upstream: upstream, released: make(chan struct{})}
	t.Cleanup(func() { ln.Close(); l.release() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go l.serve(c)
		}
	}()
	return l
}

func (l *stallingLink) stall()   { l.stalled.Store(true) }
func (l *stallingLink) release() { l.once.Do(func() { close(l.released) }) }

var redisWrites = [][]byte{[]byte("\r\nmulti\r\n"), []byte("\r\neval\r\n"), []byte("\r\nhset\r\n")}

func (l *stallingLink) serve(c net.Conn) {
	u, err := net.Dial("tcp", l.upstream)
	if err != nil {
		c.Close()
		return
	}
	go func() { _, _ = io.Copy(c, u); c.Close() }()
	held := false
	buf := make([]byte, 64<<10)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			chunk := bytes.ToLower(buf[:n])
			if !held && l.stalled.Load() && slices.ContainsFunc(redisWrites, func(w []byte) bool { return bytes.Contains(chunk, w) }) {
				held = true
			}
			if held {
				<-l.released
			}
			if _, werr := u.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	// The service may have hung up; Redis still runs what reached it.
	time.Sleep(time.Second)
	u.Close()
}

// A gated call cut off at its request timeout must have no effect, even when its write
// reaches Redis after the cut-off (Ospec add-agency-api-access, "Deadline gate": no
// effect after the cut-off). The network to Redis stalls on a signed enable of a disabled
// destination; the call is cut off with 503; the held write then reaches Redis. The
// destination must still be disabled, so a publish afterwards matches no destination and
// nothing is delivered.
func TestAGatedWriteCannotLandAfterTheCutOff(t *testing.T) {
	const timeout = 400 * time.Millisecond
	api := startGatedAPI(t, timeout)
	base := "/api/v1/tenants/" + api.tenant

	status, body := api.signed(t, "proxied_mutation", http.MethodPut, base, map[string]any{})
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status, body["_raw"])
	status, body = api.signed(t, "proxied_mutation", http.MethodPost, base+"/destinations", map[string]any{
		"type": "webhook", "topics": []string{"user.created"},
		"config": map[string]any{"url": "https://hooks.example.com/mb"},
	})
	require.Equal(t, http.StatusCreated, status, body["_raw"])
	dest, _ := body["id"].(string)
	require.NotEmpty(t, dest, body["_raw"])
	status, body = api.signed(t, "proxied_mutation", http.MethodPut, base+"/destinations/"+dest+"/disable", nil)
	require.Equal(t, http.StatusOK, status, body["_raw"])

	api.link.stall()
	status, body = api.signed(t, "proxied_mutation", http.MethodPut, base+"/destinations/"+dest+"/enable", nil)
	require.Equal(t, http.StatusServiceUnavailable, status, body["_raw"])
	require.Contains(t, body["_raw"], "cut off at its request timeout")

	// The held write reaches Redis after the cut-off.
	time.Sleep(300 * time.Millisecond)
	api.link.release()
	time.Sleep(1500 * time.Millisecond)
	status, body = api.get(t, base+"/destinations/"+dest)
	require.Equal(t, http.StatusOK, status, body["_raw"])
	require.NotNil(t, body["disabled_at"], "the enable landed after the gate cut it off: %s", body["_raw"])

	status, body = api.signed(t, "publish", http.MethodPost, "/api/v1/publish", map[string]any{
		"tenant_id": api.tenant, "topic": "user.created", "data": map[string]any{"n": 1},
	})
	require.Equal(t, http.StatusAccepted, status, body["_raw"])
	matched, ok := body["destination_ids"].([]any)
	require.True(t, ok, body["_raw"])
	require.Empty(t, matched, "a publish after the cut-off reached the destination: %s", body["_raw"])
}

// Only a webhook destination may exist (every delivery leaves through the egress proxy):
// through the built service, a signed create of every other upstream type is refused, and
// a webhook create succeeds.
func TestOnlyWebhookDestinationsCanBeCreated(t *testing.T) {
	api := startGatedAPI(t, 2*time.Second)
	base := "/api/v1/tenants/" + api.tenant
	status, body := api.signed(t, "proxied_mutation", http.MethodPut, base, map[string]any{})
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status, body["_raw"])
	for _, typ := range []string{"hookdeck", "aws_sqs", "aws_eventbridge", "aws_kinesis", "aws_s3", "gcp_pubsub", "azure_servicebus", "rabbitmq", "kafka", "cloudflare_queues"} {
		t.Run(typ, func(t *testing.T) {
			status, body := api.signed(t, "proxied_mutation", http.MethodPost, base+"/destinations", map[string]any{
				"type": typ, "topics": []string{"user.created"}, "config": map[string]any{}, "credentials": map[string]any{},
			})
			require.Equal(t, http.StatusForbidden, status, body["_raw"])
		})
	}
	status, body = api.signed(t, "proxied_mutation", http.MethodPost, base+"/destinations", map[string]any{
		"type": "webhook", "topics": []string{"user.created"}, "config": map[string]any{"url": "https://hooks.example.com/mb"},
	})
	require.Equal(t, http.StatusCreated, status, body["_raw"])
	status, body = api.signed(t, "proxied_mutation", http.MethodPatch, base+"/destinations/"+body["id"].(string), map[string]any{"type": "rabbitmq"})
	require.Equal(t, http.StatusForbidden, status, body["_raw"])
}
