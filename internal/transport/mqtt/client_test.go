package mqtt

import (
	"context"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExpirySecondsRoundsUp(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want uint32
	}{
		{30 * time.Second, 30},
		{2500 * time.Millisecond, 3}, // Rounding down would expire a scan early.
		{time.Millisecond, 1},        // Never zero, which would mean "no expiry".
		{0, 0},
		{-time.Second, 0},
	}
	for _, tc := range tests {
		if got := expirySeconds(tc.in); got != tc.want {
			t.Errorf("expirySeconds(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestKeepaliveSeconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want uint16
	}{
		{30 * time.Second, 30},
		{0, 30},                           // Zero would disable keepalive entirely.
		{-time.Second, 30},                //
		{500 * time.Millisecond, 1},       // Sub-second still has to keep alive.
		{100 * time.Hour, math.MaxUint16}, // Clamped rather than overflowed.
	}
	for _, tc := range tests {
		if got := keepaliveSeconds(tc.in); got != tc.want {
			t.Errorf("keepaliveSeconds(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// Section 4.4's rule against spinning applies to the broker as well: the delay
// has to grow, and stop at the ceiling.
func TestBackoffGrowsAndIsBounded(t *testing.T) {
	initial, maxDelay := time.Second, 8*time.Second
	b := backoff(initial, maxDelay, 0)

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for attempt, w := range want {
		if got := b(attempt); got != w {
			t.Errorf("backoff attempt %d = %s, want %s", attempt, got, w)
		}
	}
}

// Jitter is what keeps a site full of stations from reconnecting in lockstep
// after a broker restart, so it has to actually vary, and stay in range.
func TestBackoffJitterVariesWithinBounds(t *testing.T) {
	const jitter = 0.3
	b := backoff(time.Second, time.Minute, jitter)

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		got := b(0)
		if got < time.Duration(float64(time.Second)*(1-jitter)) || got > time.Duration(float64(time.Second)*(1+jitter)) {
			t.Fatalf("backoff = %s, outside 1s +/- %v%%", got, jitter*100)
		}
		seen[got] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct delays in 50 draws; the jitter is not spreading reconnects", len(seen))
	}
}

func TestTLSConfigPlaintextSchemeHasNoTLS(t *testing.T) {
	u, err := url.Parse("tcp://localhost:1883")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg, err := tlsConfig(u, "", true)
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg != nil {
		t.Error("a plaintext URL produced a TLS configuration")
	}
}

// A CA file with a plaintext URL means someone believes the connection is
// encrypted when it is not. That is worth failing over rather than ignoring.
func TestTLSConfigRejectsCAFileOnPlaintextURL(t *testing.T) {
	u, _ := url.Parse("tcp://localhost:1883")
	_, err := tlsConfig(u, "/etc/skuhus-device-agent/ca.pem", false)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "not TLS") {
		t.Errorf("error = %v, want it to say the scheme is not TLS", err)
	}
}

func TestTLSConfigUsesHostnameAndRejectsBadCA(t *testing.T) {
	u, _ := url.Parse("tls://mq.internal:8883")
	cfg, err := tlsConfig(u, "", false)
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg.ServerName != "mq.internal" {
		t.Errorf("ServerName = %q, want mq.internal", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify is set without broker.insecure")
	}
	if cfg.MinVersion != 0x0303 {
		t.Errorf("MinVersion = %#x, want TLS 1.2", cfg.MinVersion)
	}

	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(notPEM, []byte("this is not a certificate"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := tlsConfig(u, notPEM, false); err == nil {
		t.Error("expected an error for a file containing no certificates")
	}
}

func TestDialRejectsUnusableOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{"no client id", Options{URL: "tcp://localhost:1883"}, "client id is required"},
		{"unparseable url", Options{URL: "://nope", ClientID: "pack-03"}, "broker url"},
		{
			"missing ca file",
			Options{URL: "tls://mq.internal:8883", ClientID: "pack-03", CAFile: "/nonexistent/ca.pem"},
			"ca_file",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := Dial(ctx, tc.opts)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
