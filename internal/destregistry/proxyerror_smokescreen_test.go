package destregistry

import (
	"errors"
	"net/http"
	"testing"
)

// MB Wallet's egress proxy is Stripe's Smokescreen, which answers a CONNECT it denies with 407
// and X-Smokescreen-Error "Egress proxying is denied to host '<host:port>': <reason>." That
// denial is a property of the destination (its name resolved into a refused range), so it is a
// failed attempt the partner sees, never a nack retried as an infrastructure fault (Ospec
// add-agency-api-access, "Webhook destinations are safe to reach": refused by the proxy and
// recorded as a failed attempt). A 407 without that marker stays a proxy credential problem.
func TestClassifyProxyConnectResponseSmokescreenDeny(t *testing.T) {
	underlying := errors.New("proxy refused CONNECT")
	deny := func(msg string) http.Header {
		h := http.Header{}
		h.Set("X-Smokescreen-Error", msg)
		return h
	}
	cases := []struct {
		name        string
		status      int
		header      http.Header
		destination bool
	}{
		{"a Smokescreen denial of a private range", http.StatusProxyAuthRequired, deny("Egress proxying is denied to host 'private.example:443': no valid IP found among resolved addresses - 10.0.0.5 denied by rule 'Deny: Private Range'."), true},
		{"a Smokescreen denial of an IPv6 embedding", http.StatusProxyAuthRequired, deny("Egress proxying is denied to host 'nat64.example:443': no valid IP found among resolved addresses - 64:ff9b::a00:5 denied by rule 'Deny: IPv6 Embedding'."), true},
		{"a 407 with no Smokescreen marker", http.StatusProxyAuthRequired, http.Header{}, false},
		{"a 407 whose Smokescreen error is not a denial", http.StatusProxyAuthRequired, deny("An unexpected error occurred: boom"), false},
		{"a 401 carrying the denial text", http.StatusUnauthorized, deny("Egress proxying is denied to host 'x:443': y."), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ClassifyProxyConnectResponse(tc.status, tc.header, underlying, "private.example:443")
			var dest *ErrProxyDestination
			var infra *ErrProxyInfra
			if tc.destination {
				if !errors.As(err, &dest) {
					t.Fatalf("got %T (%v), want *ErrProxyDestination", err, err)
				}
				if dest.Code != "network_unreachable" || dest.DestHost != "private.example" {
					t.Fatalf("got code %q host %q, want network_unreachable private.example", dest.Code, dest.DestHost)
				}
				return
			}
			if !errors.As(err, &infra) {
				t.Fatalf("got %T (%v), want *ErrProxyInfra", err, err)
			}
		})
	}
}
