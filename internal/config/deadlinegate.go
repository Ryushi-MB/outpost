package config

// DeadlineGateConfig configures MB Wallet's deadline gate in front of the API
// (internal/deadlinegate). The gate cannot be turned off: the API refuses to start
// without a secret, a database and a request timeout.
type DeadlineGateConfig struct {
	Secret           string `yaml:"secret" env:"WEBHOOK_DEADLINE_SECRET" desc:"HMAC key for Mb-Deadline-Sig. Every non-GET API request must carry a valid signature by it. At least 32 bytes." required:"Y"`
	PreviousSecret   string `yaml:"previous_secret" env:"WEBHOOK_DEADLINE_SECRET_PREVIOUS" desc:"The previous deadline signing key, accepted as well during a rotation window only." required:"N"`
	DatabaseURL      string `yaml:"database_url" env:"WEBHOOK_DEADLINE_GATE_DATABASE_URL" desc:"Direct connection to MB Wallet's primary database as mbwallet_neon_webhook_gate, for the admission read. Must use sslmode=verify-full." required:"Y"`
	LocalPlaintext   bool   `yaml:"local_plaintext" env:"WEBHOOK_DEADLINE_GATE_LOCAL_PLAINTEXT" desc:"Local stacks only: allow a gate database URL without sslmode=verify-full when every host is this machine or the Docker host." required:"N"`
	RequestTimeoutMs int    `yaml:"request_timeout_ms" env:"WEBHOOK_DEADLINE_GATE_REQUEST_TIMEOUT_MS" desc:"The webhook service request timeout in milliseconds. An admitted call is cut off at the smaller of this and the one stored on its start record." required:"Y"`
	// AcceptanceRetentionHours is how long after its cut-off a gated publish's acceptance
	// record is kept (redis.Acceptances).
	AcceptanceRetentionHours int `yaml:"acceptance_retention_hours" env:"WEBHOOK_DEADLINE_GATE_ACCEPTANCE_RETENTION_HOURS" desc:"Hours an accepted publish's acceptance record is kept after its cut-off. At least 720 (30 days, the delivery log and partner retry window)." required:"Y"`
}
