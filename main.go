package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"xhunt-hunter/internal/app"
)

func main() {
	cfg := app.Config{}

	flag.StringVar(&cfg.DSN, "dsn", "", "MySQL DSN")
	flag.StringVar(&cfg.Domain, "domain", "web3", "XHunt domain query parameter")
	flag.StringVar(&cfg.SeedsRaw, "seeds", "", "Comma-separated seed usernames")
	flag.IntVar(&cfg.MaxDepth, "max-depth", 2, "Max BFS depth, root seeds are depth 0")
	flag.DurationVar(&cfg.RequestInterval, "request-interval", 15*time.Second, "Delay between successful requests")
	flag.DurationVar(&cfg.RateLimitSleep, "rate-limit-sleep", 65*time.Second, "Sleep duration after rate limit")
	flag.StringVar(&cfg.LogDir, "log-dir", "logs", "Directory for txt logs")
	flag.StringVar(&cfg.ImportJSON, "import-json", "", "Comma-separated json files or directories to import into MySQL before crawling")
	flag.BoolVar(&cfg.MigrateOnly, "migrate-only", false, "Only initialize schema and exit")
	flag.Parse()

	if strings.TrimSpace(cfg.DSN) == "" {
		fmt.Fprintln(os.Stderr, "-dsn is required")
		os.Exit(1)
	}
	if cfg.MaxDepth < 1 {
		fmt.Fprintln(os.Stderr, "-max-depth must be >= 1")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "xhunt-hunter failed: %v\n", err)
		os.Exit(1)
	}
}
