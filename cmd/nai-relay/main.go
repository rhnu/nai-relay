package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/rhnu/nai-relay/internal/relay"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

var version = "dev"
var commit = "unknown"

func main() {
	configPath := flag.String("config", "", "TOML configuration file (environment-only when omitted)")
	showVersion := flag.Bool("version", false, "print version")
	flag.Parse()
	if *showVersion {
		fmt.Printf("nai-relay %s (%s)\n", version, commit)
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	cfg, err := relay.LoadConfig(*configPath)
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := relay.Run(ctx, cfg); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
