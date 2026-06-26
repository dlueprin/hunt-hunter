package conf

import (
	"fmt"
	"os"
	"time"

	"github.com/928799934/json5-go"

	"xhunt-hunter/internal/app"
)

type Config struct {
	Log struct {
		Dir string `json:"dir"`
	} `json:"log"`
	Mysql struct {
		Addr string `json:"addr"`
	} `json:"mysql"`
	XHunt struct {
		Domain          string `json:"domain"`
		ProxyPort       int    `json:"proxy_port"`
		RequestTimeout  string `json:"request_timeout"`
		Seeds           string `json:"seeds"`
		ImportJSON      string `json:"import_json"`
		MaxDepth        int    `json:"max_depth"`
		ExpandRankLimit int    `json:"expand_rank_limit"`
	} `json:"xhunt"`
	Service struct {
		RequestInterval          string  `json:"request_interval"`
		RateLimitSleep           string  `json:"rate_limit_sleep"`
		FailureBackoffMultiplier float64 `json:"failure_backoff_multiplier"`
		SuccessCooldownEvery     int     `json:"success_cooldown_every"`
		SuccessCooldownSleep     string  `json:"success_cooldown_sleep"`
		SuccessCountEvery        int     `json:"success_count_every"`
		SuccessCooldownAllSleep  string  `json:"success_cooldown_all_sleep"`
	} `json:"service"`
	WiFiRecover struct {
		AfterFailures int    `json:"after_failures"`
		Mode          string `json:"mode"`
		Service       string `json:"service"`
		Device        string `json:"device"`
		SSID          string `json:"ssid"`
		Password      string `json:"password"`
		FromSSID      string `json:"from_ssid"`
		FromPassword  string `json:"from_password"`
		ToSSID        string `json:"to_ssid"`
		ToPassword    string `json:"to_password"`
		Wait          string `json:"wait"`
		PostWait      string `json:"post_wait"`
	} `json:"wifi_recover"`
	Replay struct {
		OnStart       bool  `json:"on_start"`
		SuccessDepths []int `json:"success_depths"`
		SuccessLimit  int   `json:"success_limit"`
	} `json:"replay"`
}

func Load(path string) (app.Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return app.Config{}, err
	}
	if err := json5.Unmarshal(data, &cfg); err != nil {
		return app.Config{}, err
	}

	return cfg.toAppConfig()
}

func defaultConfig() Config {
	cfg := Config{}
	cfg.Log.Dir = "logs"
	cfg.XHunt.Domain = "web3"
	cfg.XHunt.RequestTimeout = "6s"
	cfg.XHunt.MaxDepth = 2
	cfg.XHunt.ExpandRankLimit = 10000
	cfg.Service.RequestInterval = "15s"
	cfg.Service.RateLimitSleep = "65s"
	cfg.Service.FailureBackoffMultiplier = 1
	cfg.Service.SuccessCooldownSleep = "0s"
	cfg.Service.SuccessCooldownAllSleep = "0s"
	cfg.WiFiRecover.Service = "Wi-Fi"
	cfg.WiFiRecover.Wait = "3s"
	cfg.WiFiRecover.PostWait = "5s"
	return cfg
}

func (c Config) toAppConfig() (app.Config, error) {
	requestInterval, err := parseDuration("service.request_interval", c.Service.RequestInterval)
	if err != nil {
		return app.Config{}, err
	}
	requestTimeout, err := parseDuration("xhunt.request_timeout", c.XHunt.RequestTimeout)
	if err != nil {
		return app.Config{}, err
	}
	rateLimitSleep, err := parseDuration("service.rate_limit_sleep", c.Service.RateLimitSleep)
	if err != nil {
		return app.Config{}, err
	}
	successCooldownSleep, err := parseDuration("service.success_cooldown_sleep", c.Service.SuccessCooldownSleep)
	if err != nil {
		return app.Config{}, err
	}
	successCooldownAllSleep, err := parseDuration("service.success_cooldown_all_sleep", c.Service.SuccessCooldownAllSleep)
	if err != nil {
		return app.Config{}, err
	}
	wifiRecoverWait, err := parseDuration("wifi_recover.wait", c.WiFiRecover.Wait)
	if err != nil {
		return app.Config{}, err
	}
	wifiRecoverPostWait, err := parseDuration("wifi_recover.post_wait", c.WiFiRecover.PostWait)
	if err != nil {
		return app.Config{}, err
	}
	if c.Mysql.Addr == "" {
		return app.Config{}, fmt.Errorf("mysql.addr is required")
	}
	if c.XHunt.MaxDepth < 1 {
		return app.Config{}, fmt.Errorf("xhunt.max_depth must be >= 1")
	}

	return app.Config{
		DSN:                      c.Mysql.Addr,
		Domain:                   c.XHunt.Domain,
		ProxyPort:                c.XHunt.ProxyPort,
		RequestTimeout:           requestTimeout,
		SeedsRaw:                 c.XHunt.Seeds,
		MaxDepth:                 c.XHunt.MaxDepth,
		ExpandRankLimit:          c.XHunt.ExpandRankLimit,
		RequestInterval:          requestInterval,
		RateLimitSleep:           rateLimitSleep,
		FailureBackoffMultiplier: c.Service.FailureBackoffMultiplier,
		WiFiRecoverAfterFailures: c.WiFiRecover.AfterFailures,
		WiFiRecoverMode:          c.WiFiRecover.Mode,
		WiFiRecoverService:       c.WiFiRecover.Service,
		WiFiRecoverDevice:        c.WiFiRecover.Device,
		WiFiRecoverSSID:          c.WiFiRecover.SSID,
		WiFiRecoverPassword:      c.WiFiRecover.Password,
		WiFiRecoverFromSSID:      c.WiFiRecover.FromSSID,
		WiFiRecoverFromPassword:  c.WiFiRecover.FromPassword,
		WiFiRecoverToSSID:        c.WiFiRecover.ToSSID,
		WiFiRecoverToPassword:    c.WiFiRecover.ToPassword,
		WiFiRecoverWait:          wifiRecoverWait,
		WiFiRecoverPostWait:      wifiRecoverPostWait,
		ReplayOnStart:            c.Replay.OnStart,
		ReplaySuccessDepths:      c.Replay.SuccessDepths,
		ReplaySuccessLimit:       c.Replay.SuccessLimit,
		SuccessCooldownEvery:     c.Service.SuccessCooldownEvery,
		SuccessCooldownSleep:     successCooldownSleep,
		SuccessCountEvery:        c.Service.SuccessCountEvery,
		SuccessCooldownAllSleep:  successCooldownAllSleep,
		LogDir:                   c.Log.Dir,
		ImportJSON:               c.XHunt.ImportJSON,
	}, nil
}

func parseDuration(field, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s invalid duration %q: %w", field, value, err)
	}
	return duration, nil
}
