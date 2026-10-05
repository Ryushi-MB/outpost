package config_test

import (
	"strings"
	"testing"

	"github.com/hookdeck/outpost/internal/config"
	"github.com/stretchr/testify/require"
)

// The settings MB Wallet runs the fork with, as the platform lane sets them. Each row
// changes one and the service must refuse to start, naming that setting; an upstream
// default must never stand in for a missing value.
func TestMBWalletSettingsAreRequiredAtStart(t *testing.T) {
	base := map[string]string{
		"POSTGRES_URL":                                     "postgres://postgres:postgres@localhost:5432/postgres",
		"RABBITMQ_SERVER_URL":                              "amqp://localhost:5672",
		"AES_ENCRYPTION_SECRET":                            "secret",
		"DELIVERY_TIMEOUT_SECONDS":                         "10",
		"RETRY_SCHEDULE":                                   "30,120,600,1800,3600,7200,14400,28800,43200,43200,43200,43200,43200",
		"ALERT_AUTO_DISABLE_DESTINATION":                   "false",
		"DESTINATIONS_PROXY_URL":                           "http://smokescreen:4750",
		"DISABLE_TELEMETRY":                                "true",
		"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": "720",
		"DESTINATIONS_WEBHOOK_HEADER_PREFIX":               " ",
	}
	const unset = "\x00unset"
	cases := []struct {
		name    string
		env     map[string]string
		refused string // the setting the error names; empty when the service starts
	}{
		{"the lane's settings", nil, ""},
		{"the proxy set through the webhook-only setting", map[string]string{"DESTINATIONS_PROXY_URL": unset, "DESTINATIONS_WEBHOOK_PROXY_URL": "http://smokescreen:4750"}, ""},
		{"retention longer than 30 days", map[string]string{"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": "2160"}, ""},

		{"delivery timeout unset (upstream default 5 s)", map[string]string{"DELIVERY_TIMEOUT_SECONDS": unset}, "DELIVERY_TIMEOUT_SECONDS"},
		{"delivery timeout 11 s", map[string]string{"DELIVERY_TIMEOUT_SECONDS": "11"}, "DELIVERY_TIMEOUT_SECONDS"},
		{"delivery timeout 9 s", map[string]string{"DELIVERY_TIMEOUT_SECONDS": "9"}, "DELIVERY_TIMEOUT_SECONDS"},
		{"retry schedule unset (upstream exponential backoff)", map[string]string{"RETRY_SCHEDULE": unset}, "RETRY_SCHEDULE"},
		{"retry schedule missing the last retry", map[string]string{"RETRY_SCHEDULE": "30,120,600,1800,3600,7200,14400,28800,43200,43200,43200,43200"}, "RETRY_SCHEDULE"},
		{"retry schedule with a fourteenth retry", map[string]string{"RETRY_SCHEDULE": "30,120,600,1800,3600,7200,14400,28800,43200,43200,43200,43200,43200,43200"}, "RETRY_SCHEDULE"},
		{"retry schedule out of order", map[string]string{"RETRY_SCHEDULE": "120,30,600,1800,3600,7200,14400,28800,43200,43200,43200,43200,43200"}, "RETRY_SCHEDULE"},
		{"retry schedule of upstream's example", map[string]string{"RETRY_SCHEDULE": "5,60,600,3600,7200"}, "RETRY_SCHEDULE"},
		{"auto-disable on", map[string]string{"ALERT_AUTO_DISABLE_DESTINATION": "true"}, "ALERT_AUTO_DISABLE_DESTINATION"},
		{"no egress proxy", map[string]string{"DESTINATIONS_PROXY_URL": unset}, "DESTINATIONS_PROXY_URL"},
		{"a blank egress proxy", map[string]string{"DESTINATIONS_PROXY_URL": "  "}, "DESTINATIONS_PROXY_URL"},
		{"pprof on", map[string]string{"PPROF_ENABLED": "true"}, "PPROF_ENABLED"},
		{"operator events over HTTP", map[string]string{"OPERATOR_EVENTS_TOPICS": "*", "OPERATOR_EVENTS_HTTP_URL": "https://ops.example.com/hook"}, "OPERATOR_EVENTS_HTTP_URL"},
		{"operator events to SQS", map[string]string{"OPERATOR_EVENTS_TOPICS": "*", "OPERATOR_EVENTS_AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/1/q"}, "OPERATOR_EVENTS_AWS_SQS_QUEUE_URL"},
		{"operator events to Pub/Sub", map[string]string{"OPERATOR_EVENTS_TOPICS": "*", "OPERATOR_EVENTS_GCP_PUBSUB_PROJECT_ID": "p", "OPERATOR_EVENTS_GCP_PUBSUB_TOPIC_ID": "t"}, "OPERATOR_EVENTS_GCP_PUBSUB_TOPIC_ID"},
		{"operator events to RabbitMQ", map[string]string{"OPERATOR_EVENTS_TOPICS": "*", "OPERATOR_EVENTS_RABBITMQ_SERVER_URL": "amqp://ops:5672"}, "OPERATOR_EVENTS_RABBITMQ_SERVER_URL"},
		{"telemetry on (DISABLE_TELEMETRY unset)", map[string]string{"DISABLE_TELEMETRY": unset}, "DISABLE_TELEMETRY"},
		{"telemetry on (DISABLE_TELEMETRY false)", map[string]string{"DISABLE_TELEMETRY": "false"}, "DISABLE_TELEMETRY"},
		{"OpenTelemetry export on", map[string]string{"OTEL_SERVICE_NAME": "outpost"}, "OTEL_SERVICE_NAME"},
		{"acceptance retention unset", map[string]string{"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": unset}, "WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS"},
		{"acceptance retention one hour short of 30 days", map[string]string{"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": "719"}, "WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS"},
		{"acceptance retention of the old 7 days", map[string]string{"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": "168"}, "WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS"},
		{"header prefix unset (upstream default x-outpost-)", map[string]string{"DESTINATIONS_WEBHOOK_HEADER_PREFIX": unset}, "DESTINATIONS_WEBHOOK_HEADER_PREFIX"},
		{"header prefix x-outpost-", map[string]string{"DESTINATIONS_WEBHOOK_HEADER_PREFIX": "x-outpost-"}, "DESTINATIONS_WEBHOOK_HEADER_PREFIX"},
		{"header prefix Mb-", map[string]string{"DESTINATIONS_WEBHOOK_HEADER_PREFIX": "Mb-"}, "DESTINATIONS_WEBHOOK_HEADER_PREFIX"},
		{"header prefix of tabs and spaces", map[string]string{"DESTINATIONS_WEBHOOK_HEADER_PREFIX": " 	 "}, ""},
		{"acceptance retention negative", map[string]string{"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS": "-720"}, "WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			for k, v := range tc.env {
				if v == unset {
					delete(env, k)
				} else {
					env[k] = v
				}
			}
			c, err := config.ParseWithOS(config.Flags{}, &mockOS{envVars: env})
			require.NoError(t, err)
			err = c.ValidateMBWallet()
			if tc.refused == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, config.ErrMBWalletSetting)
			require.True(t, strings.Contains(err.Error(), tc.refused+" "), "the error names %s: %v", tc.refused, err)
		})
	}
}
