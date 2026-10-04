#!/usr/bin/env bash
# Break each deadline-gate check in turn and require the gate's tests to go red.
# Usage (from the repo root):
#   MB_GATE_TEST_ADMIN_URL=... MB_GATE_TEST_GATE_URL=... internal/deadlinegate/ablate.sh
# The admission-read checks need both URLs (a database with MB Wallet's schema); without
# them those tests skip, so the script refuses rather than count a skip as red.
set -euo pipefail
cd "$(dirname "$0")"
[ -n "$(printenv MB_GATE_TEST_ADMIN_URL)" ] && [ -n "$(printenv MB_GATE_TEST_GATE_URL)" ] \
  || { echo "ablate: set MB_GATE_TEST_ADMIN_URL and MB_GATE_TEST_GATE_URL" >&2; exit 2; }
backup="$(mktemp -d)"
cp gate.go admission.go "$backup/"
restore() { cp "$backup/gate.go" "$backup/admission.go" .; }
trap restore EXIT

go test -count=1 . >/dev/null || { echo "ablate: the tests are not green before ablation" >&2; exit 1; }

failed=0
# name | file | perl substitution that removes exactly one check
while IFS='|' read -r name file expr; do
  [ -n "$name" ] || continue
  restore
  perl -0pi -e "$expr" "$file"
  if cmp -s "$file" "$backup/$file"; then
    echo "NOT APPLIED  $name"; failed=1; continue
  fi
  if go test -count=1 . >/dev/null 2>&1; then
    echo "STAYED GREEN $name"; failed=1
  else
    echo "red          $name"
  fi
done <<'MUTATIONS'
unsigned non-GET passes|gate.go|s/refuse\(w, http.StatusForbidden, "unsigned call"\)/next.ServeHTTP(w, r)/
signature not verified|gate.go|s/if !g.verify\(r.Method, r.RequestURI, body, h\) \{/if false {/
body hash not signed|gate.go|s/hex.EncodeToString\(sum\[:\]\), startRecordID/"", startRecordID/
previous key equal to current accepted|gate.go|s/if previous == current \{/if false {/
repeated headers accepted|gate.go|s/if len\(vs\) != 1 \{/if len(vs) == 0 {/
body over the limit accepted|gate.go|s/if len\(body\) > MaxBodyBytes \{/if false {/
kind, deadline or record id unchecked|gate.go|s/if !callKinds\[h.kind\] \|\| !canonicalDeadline\(h.deadline\) \|\| !startRecordID.MatchString\(h.startRecord\) \{/if false {/
body tenant not compared with the path|gate.go|s/ok && t != nil && \*t != tenant/false/
query tenant not compared|gate.go|s/if v != tenant \{/if false {/
read error admits|gate.go|s/if err != nil \{\n\t\t\trefuse\(w, http.StatusServiceUnavailable, "admission read failed"\)\n\t\t\treturn\n\t\t\}//
database refusal ignored|gate.go|s/if !d.Admit \{/if false {/
cut-off ignores the stored timeout|gate.go|s/min\(g.requestTimeout, d.RequestTimeout\)/g.requestTimeout/
cut-off ignores the configured timeout|gate.go|s/min\(g.requestTimeout, d.RequestTimeout\)/d.RequestTimeout/
cut-off not measured from the timer start|gate.go|s/remaining := limit - time.Since\(start\)/remaining := limit/
read not bounded by the timeout|gate.go|s/context.WithTimeout\(r.Context\(\), g.requestTimeout-time.Since\(start\)\)/context.WithCancel(r.Context())/
short secret accepted|gate.go|s/if len\(current\) < minSecretBytes \{/if false {/
no database accepted|gate.go|s/if admitter == nil \{/if false {/
empty database URL accepted|admission.go|s/if url == "" \{/if false {/
D not checked|admission.go|s/\n\s+AND clock_timestamp\(\) < s.deadline//
signed D not compared with stored D|admission.go|s/\n\s+AND s.deadline = \$3::timestamptz//
kind not compared|admission.go|s/\n\s+AND s.kind = \$2//
tenant not compared|admission.go|s/\n\s+AND s.org_id::text \|\| ':' \|\| s.mode = \$4//
access On not required|admission.go|s/THEN a.state = 'on'/THEN true/
job state not compared|admission.go|s/THEN a.state = s.access_state/THEN true/
cycle marker not compared|admission.go|s/\n\s+AND a.cycle_marker = s.cycle_marker//
lease token not compared|admission.go|s/\n\s+AND s.lease_token = l.fencing_token//
MUTATIONS
exit "$failed"
