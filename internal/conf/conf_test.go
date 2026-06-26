package conf

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadParsesConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json5")
	data := []byte(`{
  "mysql": {
    "addr": "user:pass@tcp(localhost:3306)/hot"
  },
  "xhunt": {
    "max_depth": 20,
    "expand_rank_limit": 10000
  },
  "service": {
    "request_interval": "400ms",
    "rate_limit_sleep": "3300s",
    "failure_backoff_multiplier": 2,
    "success_cooldown_every": 100,
    "success_cooldown_sleep": "100ms"
  },
  "wifi_recover": {
    "after_failures": 1,
    "mode": "double-hop",
    "device": "en1",
    "from_ssid": "primary",
    "from_password": "primary-pass",
    "to_ssid": "secondary",
    "to_password": "secondary-pass",
    "wait": "3s",
    "post_wait": "5s"
  }
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.DSN != "user:pass@tcp(localhost:3306)/hot" {
		t.Fatalf("unexpected dsn: %s", cfg.DSN)
	}
	if cfg.MaxDepth != 20 {
		t.Fatalf("unexpected max depth: %d", cfg.MaxDepth)
	}
	if cfg.RequestInterval != 400*time.Millisecond {
		t.Fatalf("unexpected request interval: %s", cfg.RequestInterval)
	}
	if cfg.RateLimitSleep != 55*time.Minute {
		t.Fatalf("unexpected rate limit sleep: %s", cfg.RateLimitSleep)
	}
	if cfg.WiFiRecoverPostWait != 5*time.Second {
		t.Fatalf("unexpected wifi post wait: %s", cfg.WiFiRecoverPostWait)
	}
}
