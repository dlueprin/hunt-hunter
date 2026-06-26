package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"xhunt-hunter/internal/wifi"
)

type config struct {
	Service      string
	Mode         string
	Device       string
	SSID         string
	Password     string
	FromSSID     string
	FromPassword string
	ToSSID       string
	ToPassword   string
	Wait         time.Duration
	DryRun       bool
}

func main() {
	cfg := config{}

	flag.StringVar(&cfg.Service, "service", "Wi-Fi", "macOS network service name, usually Wi-Fi")
	flag.StringVar(&cfg.Mode, "mode", "power-cycle", "test mode: power-cycle or reconnect")
	flag.StringVar(&cfg.Device, "device", "en0", "hardware device name for reconnect mode, usually en0")
	flag.StringVar(&cfg.SSID, "ssid", "", "target SSID when mode=reconnect")
	flag.StringVar(&cfg.Password, "password", "", "target SSID password when mode=reconnect")
	flag.StringVar(&cfg.FromSSID, "from-ssid", "", "source SSID when mode=double-hop")
	flag.StringVar(&cfg.FromPassword, "from-password", "", "source SSID password when mode=double-hop")
	flag.StringVar(&cfg.ToSSID, "to-ssid", "", "temporary target SSID when mode=double-hop")
	flag.StringVar(&cfg.ToPassword, "to-password", "", "temporary target SSID password when mode=double-hop")
	flag.DurationVar(&cfg.Wait, "wait", 3*time.Second, "wait duration between disconnect and reconnect")
	flag.BoolVar(&cfg.DryRun, "dry-run", true, "print commands without executing them")
	flag.Parse()

	wifiCfg := wifi.Config{
		Service:      cfg.Service,
		Mode:         cfg.Mode,
		Device:       cfg.Device,
		SSID:         cfg.SSID,
		Password:     cfg.Password,
		FromSSID:     cfg.FromSSID,
		FromPassword: cfg.FromPassword,
		ToSSID:       cfg.ToSSID,
		ToPassword:   cfg.ToPassword,
		Wait:         cfg.Wait,
	}

	steps, err := wifi.BuildSteps(wifiCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wifi-toggle-test: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("wifi-toggle-test starting mode=%s service=%s device=%s wait=%s dry_run=%t\n", cfg.Mode, cfg.Service, cfg.Device, cfg.Wait, cfg.DryRun)
	for idx, step := range steps {
		fmt.Printf("[%d/%d] %s\n", idx+1, len(steps), strings.Join(step, " "))
		if cfg.DryRun {
			continue
		}
		if err := wifi.Run(wifiCfg, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "wifi-toggle-test: step failed: %v\n", err)
			os.Exit(1)
		}
		break
	}
}
