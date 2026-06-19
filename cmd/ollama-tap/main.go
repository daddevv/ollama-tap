package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daddevv/ollama-tap/internal/config"
	"github.com/daddevv/ollama-tap/internal/health"
	"github.com/daddevv/ollama-tap/internal/metrics"
	"github.com/daddevv/ollama-tap/internal/proxy"
)

func main() {
	config.RegisterFlags()
	flag.Parse()

	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	m := metrics.New()
	p, err := proxy.New(cfg, m)
	if err != nil {
		log.Fatalf("proxy init: %v", err)
	}

	h := health.NewHealthHandler(m)

	mux := http.NewServeMux()
	mux.Handle("/_tap/health", h)
	mux.Handle("/_tap/stats", h)
	mux.Handle("/", p)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("ollama-tap listening on %s → upstream=%s", cfg.ListenAddr, cfg.Upstream.String())

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("ollama-tap: server shutdown error: %v", err)
		}
	}()

	if err = srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}

	log.Println("stopped")
}
