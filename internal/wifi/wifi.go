package wifi

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

type Config struct {
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
}

func BuildSteps(cfg Config) ([][]string, error) {
	service := strings.TrimSpace(cfg.Service)
	if service == "" {
		service = "Wi-Fi"
	}

	switch strings.TrimSpace(cfg.Mode) {
	case "power-cycle":
		return [][]string{
			{"networksetup", "-setairportpower", service, "off"},
			{"networksetup", "-setairportpower", service, "on"},
		}, nil
	case "reconnect":
		ssid := strings.TrimSpace(cfg.SSID)
		device := strings.TrimSpace(cfg.Device)
		if ssid == "" {
			return nil, fmt.Errorf("ssid is required when mode=reconnect")
		}
		if device == "" {
			return nil, fmt.Errorf("device is required when mode=reconnect")
		}
		return [][]string{{"networksetup", "-setairportnetwork", device, ssid, cfg.Password}}, nil
	case "double-hop":
		device := strings.TrimSpace(cfg.Device)
		fromSSID := strings.TrimSpace(cfg.FromSSID)
		toSSID := strings.TrimSpace(cfg.ToSSID)
		if device == "" {
			return nil, fmt.Errorf("device is required when mode=double-hop")
		}
		if fromSSID == "" {
			return nil, fmt.Errorf("from-ssid is required when mode=double-hop")
		}
		if toSSID == "" {
			return nil, fmt.Errorf("to-ssid is required when mode=double-hop")
		}
		return [][]string{
			{"networksetup", "-setairportnetwork", device, toSSID, cfg.ToPassword},
			{"networksetup", "-setairportnetwork", device, fromSSID, cfg.FromPassword},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", cfg.Mode)
	}
}

func Run(cfg Config, stdout, stderr io.Writer) error {
	steps, err := BuildSteps(cfg)
	if err != nil {
		return err
	}

	for idx, step := range steps {
		cmd := exec.Command(step[0], step[1:]...)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("step %d failed: %w", idx+1, err)
		}
		if idx < len(steps)-1 && cfg.Wait > 0 {
			time.Sleep(cfg.Wait)
		}
	}

	return nil
}
