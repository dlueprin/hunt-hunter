package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"xhunt-hunter/internal/app"
	"xhunt-hunter/internal/conf"
)

func main() {
	var configPath string
	var migrateOnly bool

	flag.StringVar(&configPath, "config", "config.json5", "Path to JSON5 config file")
	flag.BoolVar(&migrateOnly, "migrate-only", false, "Only initialize schema and exit")
	flag.Parse()

	cfg, err := conf.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}
	cfg.MigrateOnly = migrateOnly

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "xhunt-hunter failed: %v\n", err)
		os.Exit(1)
	}
}
