package services_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hookdeck/outpost/internal/config"
	"github.com/hookdeck/outpost/internal/logging"
	"github.com/hookdeck/outpost/internal/proxychain/proxychaintest"
	"github.com/hookdeck/outpost/internal/services"
	"github.com/hookdeck/outpost/internal/telemetry"
	"github.com/hookdeck/outpost/internal/util/testinfra"
	"github.com/hookdeck/outpost/internal/util/testutil"
	"github.com/stretchr/testify/require"
)

// mbWalletSettings gives c the settings every MB Wallet service starts with
// (config.ValidateMBWallet). Deliveries leave through an in-process forward proxy, as they
// leave through Smokescreen in the lane.
func mbWalletSettings(t *testing.T, c *config.Config) {
	t.Helper()
	c.DeliveryTimeoutSeconds = config.MBWalletDeliveryTimeoutSeconds
	c.RetrySchedule = slices.Clone(config.MBWalletRetrySchedule)
	c.Destinations.ProxyURL = proxychaintest.New(t, false).URL
	c.Telemetry.Disabled = true
	c.DeadlineGate.AcceptanceRetentionHours = 720
}

// Every service type refuses to start outside MB Wallet's settings, before it opens any
// connection (config.ValidateMBWallet owns the rules, one table row each).
func TestEveryServiceRefusesToStartOutsideMBWalletSettings(t *testing.T) {
	for _, service := range []config.ServiceType{config.ServiceTypeAll, config.ServiceTypeAPI, config.ServiceTypeDelivery, config.ServiceTypeLog} {
		t.Run(service.String(), func(t *testing.T) {
			c := &config.Config{}
			c.InitDefaults()
			mbWalletSettings(t, c)
			c.Service = service.String()
			c.RetrySchedule = nil // upstream's exponential backoff
			// Addresses nothing listens on: the service must refuse before it dials any.
			c.PostgresURL = "postgres://outpost@127.0.0.1:1/outpost?sslmode=disable"
			c.MQs.RabbitMQ.ServerURL = "amqp://guest:guest@127.0.0.1:1"
			c.Redis.Host, c.Redis.Port = "127.0.0.1", 1
			c.AESEncryptionSecret = "encryptionsecret"
			require.NoError(t, c.Validate(config.Flags{}))
			logger, err := logging.NewLogger(logging.WithLogLevel("error"))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			builder := services.NewServiceBuilder(ctx, c, logger, telemetry.New(logger, c.Telemetry.ToTelemetryConfig(), ""))
			t.Cleanup(func() { builder.Cleanup(context.Background()) })
			_, err = builder.BuildWorkers()
			require.ErrorIs(t, err, config.ErrMBWalletSetting)
			require.ErrorContains(t, err, "RETRY_SCHEDULE")
		})
	}
}

// The API service the builder starts runs behind MB Wallet's deadline gate, and its HTTP
// server bounds the time a caller may take to send a request. Needs Docker (RabbitMQ and
// Postgres for the log store, started by testinfra) and MB_GATE_TEST_GATE_URL, a database
// with MB Wallet's schema reached as mbwallet_neon_webhook_gate.
func TestAPIServiceRunsBehindTheDeadlineGate(t *testing.T) {
	testutil.CheckIntegrationTest(t)
	gateURL := os.Getenv("MB_GATE_TEST_GATE_URL")
	if gateURL == "" {
		t.Skip("MB_GATE_TEST_GATE_URL is unset: the gate needs a database with MB Wallet's schema")
	}
	redisConfig := testutil.CreateTestRedisConfig(t)
	c := &config.Config{}
	c.InitDefaults()
	c.Service = config.ServiceTypeAPI.String()
	c.APIPort = testutil.RandomPortNumber()
	c.APIKey = "apikey"
	c.APIJWTSecret = "jwtsecret"
	c.AESEncryptionSecret = "encryptionsecret"
	c.Topics = testutil.TestTopics
	c.Telemetry.Disabled = true
	c.Redis.Host, c.Redis.Port = redisConfig.Host, redisConfig.Port
	c.MQs.RabbitMQ.ServerURL = testinfra.EnsureRabbitMQ()
	c.PostgresURL = testinfra.NewPostgresConfig(t)
	c.DeadlineGate.Secret = "deadline-gate-wiring-secret-0123456789abcdef"
	c.DeadlineGate.DatabaseURL = gateURL
	c.DeadlineGate.LocalPlaintext = true
	c.DeadlineGate.RequestTimeoutMs = 500
	mbWalletSettings(t, c)
	require.NoError(t, c.Validate(config.Flags{}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := logging.NewLogger(logging.WithLogLevel("error"))
	require.NoError(t, err)
	builder := services.NewServiceBuilder(ctx, c, logger, telemetry.New(logger, c.Telemetry.ToTelemetryConfig(), ""))
	supervisor, err := builder.BuildWorkers()
	require.NoError(t, err)
	t.Cleanup(func() { builder.Cleanup(context.Background()) })
	go func() { _ = supervisor.Run(ctx) }()

	addr := fmt.Sprintf("127.0.0.1:%d", c.APIPort)
	require.Eventually(t, func() bool {
		res, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)

	t.Run("an unsigned call with the admin key is refused by the gate", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/v1/publish",
			strings.NewReader(`{"tenant_id":"t:live","topic":"user.created","data":{}}`))
		req.Header.Set("Authorization", "Bearer apikey")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		require.Equal(t, http.StatusForbidden, res.StatusCode, string(body))
		require.Contains(t, string(body), "deadline gate: unsigned call")
	})

	// A caller that claims a signature and then withholds the body it announced must not
	// hold the connection past the request timeout: the gate reads the body before it can
	// check the signature.
	t.Run("a withheld body is cut off at the request timeout", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer conn.Close()
		_, err = io.WriteString(conn, "POST /api/v1/publish HTTP/1.1\r\nHost: outpost\r\nContent-Length: 100\r\n"+
			"Mb-Deadline: d\r\nMb-Deadline-Sig: s\r\nMb-Start-Record: r\r\nMb-Call-Kind: publish\r\n\r\n{")
		require.NoError(t, err)
		start := time.Now()
		require.NoError(t, conn.SetReadDeadline(start.Add(3*time.Second)))
		_, err = bufio.NewReader(conn).ReadString('\n')
		took := time.Since(start)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("the server still held the connection after %v; the request timeout is 500ms", took)
		}
		require.Less(t, took, 1500*time.Millisecond)
	})
}
