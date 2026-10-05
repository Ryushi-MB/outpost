package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// MB Wallet runs this fork only with the delivery rules and egress of Ospec
// add-agency-api-access ("Delivery retries last at least three days", "Destinations and
// egress"). Each setting below has one allowed value, so a missing or changed setting
// stops the service instead of falling back to an upstream default.

// ErrMBWalletSetting is the error every refused setting wraps.
var ErrMBWalletSetting = errors.New("MB Wallet setting refused")

// MBWalletRetrySchedule is the spec's thirteen retries: 30 s, 2 m, 10 m, 30 m, 1 h, 2 h,
// 4 h, 8 h, then 12 h five times (272,550 s).
var MBWalletRetrySchedule = []int{30, 120, 600, 1800, 3600, 7200, 14400, 28800, 43200, 43200, 43200, 43200, 43200}

const (
	// MBWalletDeliveryTimeoutSeconds: a 2xx within 10 seconds acknowledges.
	MBWalletDeliveryTimeoutSeconds = 10
	// MBWalletMinAcceptanceRetention: partners can retry for 30 days and the delivery log
	// is kept 30 days, so an accepted publish's record must last at least that long.
	MBWalletMinAcceptanceRetention = 30 * 24 * time.Hour
)

// ValidateMBWallet refuses a configuration MB Wallet does not run: the spec's delivery
// rules not set exactly, deliveries not sent through the egress proxy, or a network path
// around the deadline gate and the proxy (profiling endpoints, operator event sinks,
// telemetry). Every service type checks it at start.
func (c *Config) ValidateMBWallet() error {
	refuse := func(setting, format string, args ...any) error {
		return fmt.Errorf("%w: %s %s", ErrMBWalletSetting, setting, fmt.Sprintf(format, args...))
	}
	if c.DeliveryTimeoutSeconds != MBWalletDeliveryTimeoutSeconds {
		return refuse("DELIVERY_TIMEOUT_SECONDS", "is %d, must be %d", c.DeliveryTimeoutSeconds, MBWalletDeliveryTimeoutSeconds)
	}
	if !slices.Equal(c.RetrySchedule, MBWalletRetrySchedule) {
		return refuse("RETRY_SCHEDULE", "is %v, must be %v", c.RetrySchedule, MBWalletRetrySchedule)
	}
	if c.Alert.AutoDisableDestination {
		return refuse("ALERT_AUTO_DISABLE_DESTINATION", "is true: the service must never disable a failing destination")
	}
	if strings.TrimSpace(c.Destinations.ProxyURL) == "" && strings.TrimSpace(c.Destinations.Webhook.ProxyURL) == "" {
		return refuse("DESTINATIONS_PROXY_URL", "is unset: every delivery must leave through the egress proxy")
	}
	// MB Wallet publishes the event's body id as the metadata key Mb-Event-Id, and every
	// delivery must carry it under that exact header name. A metadata header is the prefix
	// plus the key; whitespace is how Outpost takes an empty prefix (unset means x-outpost-).
	if p := c.Destinations.Webhook.HeaderPrefix; p == "" || strings.TrimSpace(p) != "" {
		return refuse("DESTINATIONS_WEBHOOK_HEADER_PREFIX", "is %q, must be whitespace (no prefix), so metadata Mb-Event-Id is sent as the header Mb-Event-Id", p)
	}
	if c.PprofEnabled {
		return refuse("PPROF_ENABLED", "is true: profiling endpoints are served outside the deadline gate")
	}
	oe := c.OperatorEvents
	for setting, value := range map[string]string{
		"OPERATOR_EVENTS_HTTP_URL":            oe.HTTP.URL,
		"OPERATOR_EVENTS_AWS_SQS_QUEUE_URL":   oe.AWSSQS.QueueURL,
		"OPERATOR_EVENTS_GCP_PUBSUB_TOPIC_ID": oe.GCPPubSub.TopicID,
		"OPERATOR_EVENTS_RABBITMQ_SERVER_URL": oe.RabbitMQ.ServerURL,
	} {
		if strings.TrimSpace(value) != "" {
			return refuse(setting, "is set: an operator event sink sends outside the egress proxy")
		}
	}
	if !c.DisableTelemetry && !c.Telemetry.Disabled {
		return refuse("DISABLE_TELEMETRY", "is false: telemetry sends outside the egress proxy")
	}
	if c.OpenTelemetry.ServiceName != "" {
		return refuse("OTEL_SERVICE_NAME", "is set: OpenTelemetry exports outside the egress proxy")
	}
	if retention := time.Duration(c.DeadlineGate.AcceptanceRetentionHours) * time.Hour; retention < MBWalletMinAcceptanceRetention {
		return refuse("WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS", "is %d, must be at least %d", c.DeadlineGate.AcceptanceRetentionHours, int(MBWalletMinAcceptanceRetention.Hours()))
	}
	return nil
}
