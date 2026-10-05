#!/usr/bin/env bash
# Break each deadline-gate check in turn and require the tests that own it to go red.
# Usage (from anywhere):
#   MB_GATE_TEST_ADMIN_URL=... MB_GATE_TEST_GATE_URL=... MB_GATE_TEST_STANDBY_URL=... \
#   MB_GATE_TEST_GATE_TLS_URL=... internal/deadlinegate/ablate.sh
#   ADMIN_URL    the test database (MB Wallet's schema) as its owner
#   GATE_URL     the same database as mbwallet_neon_webhook_gate, plaintext, on this machine
#   STANDBY_URL  a streaming standby of it, as mbwallet_neon_webhook_gate
#   GATE_TLS_URL the primary as mbwallet_neon_webhook_gate with sslmode=verify-full
# The database checks skip without them, and the wiring rows (./internal/services) need
# Docker, as do the write-fence rows (./internal/redis, ./internal/services: Redis Stack),
# so the script refuses rather than count a skip as red.
set -euo pipefail
cd "$(dirname "$0")/../.."
for v in MB_GATE_TEST_ADMIN_URL MB_GATE_TEST_GATE_URL MB_GATE_TEST_STANDBY_URL MB_GATE_TEST_GATE_TLS_URL; do
  [ -n "$(printenv "$v")" ] || { echo "ablate: set $v" >&2; exit 2; }
done
files=(internal/deadlinegate/gate.go internal/deadlinegate/admission.go internal/config/validation.go internal/services/builder.go internal/redis/fence.go internal/redis/redis.go internal/destregistry/providers/default.go internal/destregistry/httpclient.go internal/destregistry/providers/destwebhook/httphelper.go internal/publishmq/eventhandler.go internal/deliverymq/messagehandler.go)
backup="$(mktemp -d)"
for f in "${files[@]}"; do mkdir -p "$backup/$(dirname "$f")"; cp "$f" "$backup/$f"; done
restore() { for f in "${files[@]}"; do cp "$backup/$f" "$f"; done; }
trap restore EXIT

pkgs=(./internal/deadlinegate ./internal/config ./internal/services ./internal/redis ./internal/destregistry/providers ./internal/destregistry/providers/destwebhook)
# The packages that start containers run only the tests that own this script's rows, so a
# row does not rerun upstream's container suites (a loaded Docker Desktop makes those slow
# and flaky). Every other package runs whole.
declare -A only=(
  [./internal/services]='^(TestAPIServiceRunsBehindTheDeadlineGate|TestAGatedWriteCannotLandAfterTheCutOff|TestOnlyWebhookDestinationsCanBeCreated|TestALateEnqueueIsNeverDelivered)$'
  [./internal/redis]='^TestWriteFence$'
)
gotest() { go test -count=1 ${only[$1]:+-run "${only[$1]}"} "$1"; }
for p in "${pkgs[@]}"; do
  gotest "$p" >/dev/null || { echo "ablate: $p is not green before ablation" >&2; exit 1; }
done

failed=0
# name | file | package that must go red | perl substitution that removes exactly one check
while IFS='|' read -r name file pkg expr; do
  [ -n "$name" ] || continue
  restore
  perl -0pi -e "$expr" "$file"
  if cmp -s "$file" "$backup/$file"; then
    echo "NOT APPLIED  $name"; failed=1; continue
  fi
  # A mutant that does not compile proves nothing about the tests.
  if ! go vet "$pkg" >/dev/null 2>&1; then
    echo "NO BUILD     $name"; failed=1; continue
  fi
  if gotest "$pkg" >/dev/null 2>&1; then
    echo "STAYED GREEN $name"; failed=1
  else
    echo "red          $name"
  fi
done <<'MUTATIONS'
unsigned non-GET passes|internal/deadlinegate/gate.go|./internal/deadlinegate|s/refuse\(w, http.StatusForbidden, "unsigned call"\)/next.ServeHTTP(w, r)/
signature not verified|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if !g.verify\(r.Method, r.RequestURI, body, h\) \{/if false {/
query string not signed (RequestURI -> URL.Path)|internal/deadlinegate/gate.go|./internal/deadlinegate|s/g\.verify\(r\.Method, r\.RequestURI, body, h\)/g.verify(r.Method, r.URL.Path, body, h)/
body hash not signed|internal/deadlinegate/gate.go|./internal/deadlinegate|s/hex.EncodeToString\(sum\[:\]\), startRecordID/hex.EncodeToString(sum[:0]), startRecordID/
previous key never tried|internal/deadlinegate/gate.go|./internal/deadlinegate|s/for _, key := range g\.keys \{/for _, key := range g.keys[:1] {/
previous key equal to current accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if previous == current \{/if false {/
repeated headers accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if len\(vs\) != 1 \{/if len(vs) == 0 {/
body over the limit accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if len\(body\) > MaxBodyBytes \{/if false {/
deadline or record id unchecked|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if !canonicalDeadline\(h.deadline\) \|\| !startRecordID.MatchString\(h.startRecord\) \{/if false {/
kind not tied to the route|internal/deadlinegate/gate.go|./internal/deadlinegate|s/!slices\.Contains\(route\.kinds, kind\)/!slices.Contains(route.kinds, route.kinds[0])/
route literal segments not compared|internal/deadlinegate/gate.go|./internal/deadlinegate|s/case want != ":id" && want != segments\[i\]:/case false:/
empty path segment accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/case segments\[i\] == "":\n\t\t\t\tmatched = false\n//
unmatched route passes|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if !matched \{\n\t\treturn "", false\n\t\}\n\tif r\.URL/if r.URL/
empty tenant accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if tenant == "" \{\n\t\treturn "", false\n\t\}//
body tenant not compared with the path|internal/deadlinegate/gate.go|./internal/deadlinegate|s/ok && t != nil && \*t != tenant/ok \&\& t != nil \&\& false/
query tenant not compared|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if v != tenant \{/if v != tenant \&\& false {/
read error admits|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if err != nil \{\n\t\t\trefuse\(w, http.StatusServiceUnavailable, "admission read failed"\)\n\t\t\treturn\n\t\t\}//
database refusal ignored|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if !d.Admit \{/if false {/
cut-off ignores the stored timeout|internal/deadlinegate/gate.go|./internal/deadlinegate|s/min\(g.requestTimeout, d.RequestTimeout\)/g.requestTimeout/
cut-off ignores the configured timeout|internal/deadlinegate/gate.go|./internal/deadlinegate|s/min\(g.requestTimeout, d.RequestTimeout\)/d.RequestTimeout/
cut-off not measured from the timer start|internal/deadlinegate/gate.go|./internal/deadlinegate|s/cutOff := start\.Add\(limit\)/cutOff := time.Now().Add(limit)/
spent timeout still starts the service (remaining <= 0)|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if remaining <= 0 \{/if false {/
no cut-off (TimeoutHandler removed)|internal/deadlinegate/gate.go|./internal/deadlinegate|s/http\.TimeoutHandler\(next, remaining, cutOffBody\)\.ServeHTTP\(w, r\)/next.ServeHTTP(w, r)/
body not replayed to the service|internal/deadlinegate/gate.go|./internal/deadlinegate|s/r\.Body = io\.NopCloser\(bytes\.NewReader\(body\)\)/r.Body = io.NopCloser(bytes.NewReader(nil))/
read not bounded by the timeout|internal/deadlinegate/gate.go|./internal/deadlinegate|s/context.WithTimeout\(r.Context\(\), g.requestTimeout-time.Since\(start\)\)/context.WithCancel(r.Context())/
short secret accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if len\(current\) < minSecretBytes \{/if false {/
no database accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if admitter == nil \{/if false {/
empty database URL accepted|internal/deadlinegate/admission.go|./internal/deadlinegate|s/if url == "" \{/if false {/
verify-full not required|internal/deadlinegate/admission.go|./internal/deadlinegate|s/if t\.TLSConfig == nil \|\| t\.TLSConfig\.InsecureSkipVerify \{/if false {/
local waiver allows any host|internal/deadlinegate/admission.go|./internal/deadlinegate|s/if !localHost\(t\.Host\) \{/if false {/
gate role not checked on connect|internal/deadlinegate/admission.go|./internal/deadlinegate|s/if session != GateRole \|\| current != GateRole \{/if false {/
standby accepted at start|internal/deadlinegate/admission.go|./internal/deadlinegate|s/if standby \{/if false {/
replica check removed (pg_is_in_recovery)|internal/deadlinegate/admission.go|./internal/deadlinegate|s/NOT pg_is_in_recovery\(\)\s+AND //
D not checked|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND clock_timestamp\(\) < s.deadline//
signed D not compared with stored D|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND s.deadline = \$3::timestamptz//
kind not compared|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND s.kind = \$2//
tenant not compared|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND s.org_id::text \|\| ':' \|\| s.mode = \$4//
mode dropped from the tenant (always live)|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\|\| ':' \|\| s\.mode = \$4/|| ':live' = \$4/
access On not required|internal/deadlinegate/admission.go|./internal/deadlinegate|s/THEN a.state = 'on'/THEN true/
job state not compared|internal/deadlinegate/admission.go|./internal/deadlinegate|s/THEN a.state = s.access_state/THEN true/
cycle marker not compared|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND a.cycle_marker = s.cycle_marker//
lease token not compared|internal/deadlinegate/admission.go|./internal/deadlinegate|s/\n\s+AND s.lease_token = l.fencing_token//
NULL answer admits|internal/deadlinegate/admission.go|./internal/deadlinegate|s/END,\n\s+false\),/END,\n         true),/
no start record admits|internal/deadlinegate/admission.go|./internal/deadlinegate|s/return Decision\{\}, nil\n\t\}/return Decision{Admit: true, RequestTimeout: time.Hour}, nil\n\t}/
publish queue accepted at start|internal/config/validation.go|./internal/config|s/if c\.PublishMQ\.GetQueueConfig\(\) != nil \{/if false {/
gate not wrapped around the API|internal/services/builder.go|./internal/services|s/gate\.Wrap\(apiHandler\)/func() http.Handler { _ = gate; return apiHandler }()/
no server read timeout|internal/services/builder.go|./internal/services|s/\t\tReadTimeout: time\.Duration\(b\.cfg\.DeadlineGate\.RequestTimeoutMs\) \* time\.Millisecond,\n//
write fence not handed to the service|internal/deadlinegate/gate.go|./internal/services|s/r = r\.WithContext\(fenced\)/_ = fenced/
write fence failure ignored|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if err != nil \{\n\t\t\trefuse\(w, http.StatusServiceUnavailable, "write fence unavailable"\)/if false \&\& err != nil {\n\t\t\trefuse(w, http.StatusServiceUnavailable, "write fence unavailable")/
no write fence accepted|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if fencer == nil \{/if false {/
Redis clock not checked before the write|internal/redis/fence.go|./internal/redis|s/if now >= tonumber\(ARGV\[1\]\) then/if false then/
fence not tied to the cut-off|internal/redis/fence.go|./internal/redis|s/until: now\.UnixMicro\(\) \+ left\.Microseconds\(\)/until: now.UnixMicro() + left.Microseconds() + 3600000000/
unknown write passes under a fence|internal/redis/fence.go|./internal/redis|s/if !known \{/if !known \&\& false {/
fence hook not installed|internal/redis/redis.go|./internal/redis|s/hooked\.AddHook\(fenceHook\{\}\)/_ = hooked/
Redis ignores the call's deadline (ContextTimeoutEnabled)|internal/redis/redis.go|./internal/redis|s/\/\/ See createClusterClient\.\n\t\tContextTimeoutEnabled: true,\n//
destination type not checked at the gate|internal/deadlinegate/gate.go|./internal/deadlinegate|s/if !destinationTypeAllowed\(r, body\) \{/if false {/
destination type optional on create|internal/deadlinegate/gate.go|./internal/deadlinegate|s/return !route\.typeRequired/return true/
a non-webhook provider registered|internal/destregistry/providers/default.go|./internal/destregistry/providers|s/registry\.RegisterProvider\("webhook", webhook\)/registry.RegisterProvider("webhook", webhook)\n\tregistry.RegisterProvider("rabbitmq", webhook)/
redirects followed|internal/destregistry/httpclient.go|./internal/destregistry/providers/destwebhook|s/CheckRedirect: func\(\*http\.Request, \[\]\*http\.Request\) error \{ return http\.ErrUseLastResponse \},//
a 3xx counted as delivered|internal/destregistry/providers/destwebhook/httphelper.go|./internal/destregistry/providers/destwebhook|s/if resp\.StatusCode < 200 \|\| resp\.StatusCode >= 300 \{/if resp.StatusCode >= 400 {/
late enqueue: tasks not stamped with their acceptance|internal/publishmq/eventhandler.go|./internal/services|s/task\.Acceptance = acceptance/_ = acceptance/
late enqueue: acceptance record never written|internal/redis/fence.go|./internal/services|s/return a\.Client\.Set\(ctx, p\.Key, "1", acceptanceTTL\)\.Err\(\)/return nil/
late enqueue: consumer delivers an unaccepted task|internal/deliverymq/messagehandler.go|./internal/services|s/if !accepted \{/if !accepted \&\& false {/
late enqueue: publish acceptor not wired|internal/services/builder.go|./internal/services|s/\t\tpublishmq\.WithAcceptor\(redis\.Acceptances\{Client: svc\.redisClient\}\),\n//
late enqueue: consumer acceptance check not wired|internal/services/builder.go|./internal/services|s/\t\tdeliverymq\.WithAcceptance\(redis\.Acceptances\{Client: svc\.redisClient\}\),\n//
late enqueue: a missing record is never final|internal/redis/fence.go|./internal/redis|s/if now >= acc\.Fence \{/if now >= acc.Fence \&\& false {/
MUTATIONS
exit "$failed"
