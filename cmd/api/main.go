// Package main is the entrypoint for the Transactional Outbox API server.
// It wires together configuration, database, Kafka producer, the background
// outbox publisher, and the HTTP server, then waits for a shutdown signal.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"

	"transactional-outbox/internal/config"
	"transactional-outbox/internal/db"
	"transactional-outbox/internal/db/sqlc"
	httpapi "transactional-outbox/internal/http"
	"transactional-outbox/internal/outbox"
)

func main() {
	// Root context — cancelled on shutdown to stop all background goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration from config.yaml (path overridable via CONFIG_PATH env var).
	// Environment variables take precedence over YAML values.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	// Initialize PostgreSQL connection pool with configured pool limits.
	pool, err := db.NewPool(ctx, cfg.DB)
	if err != nil {
		log.Fatalf("db init failed: %v", err)
	}
	defer pool.Close()

	// sqlc.Queries provides type-safe database access generated from sql/queries.sql.
	q := sqlc.New(pool)

	// Initialize the idempotent Kafka producer (enable.idempotence=true, acks=all).
	prod, err := outbox.NewKafkaProducer(cfg.Kafka)
	if err != nil {
		log.Fatalf("kafka producer init failed: %v", err)
	}
	defer prod.Close()

	// Start the background outbox publisher as a goroutine.
	// It polls outbox_events every cfg.Outbox.Interval, claims a batch using
	// FOR UPDATE SKIP LOCKED, publishes each event to Kafka, and marks them SENT.
	pub := &outbox.Publisher{
		Pool:        pool,
		Q:           q,
		Producer:    prod,
		WorkerID:    hostname(),
		BatchSize:   cfg.Outbox.BatchSize,
		Interval:    cfg.Outbox.Interval,
		MaxAttempts: cfg.Outbox.MaxAttempts,
	}
	go pub.Run(ctx)

	// Set up Gin HTTP router with panic recovery middleware.
	r := gin.New()
	r.Use(gin.Recovery())

	// Register HTTP routes: POST /orders and GET /health.
	h := &httpapi.OrdersHandler{Pool: pool, Q: q}
	h.Register(r)

	srv := &http.Server{
		Addr:              cfg.Server.Port,
		Handler:           r,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
	}

	// Start HTTP server in a separate goroutine so main can wait for signals.
	go func() {
		log.Printf("HTTP listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Block until SIGINT or SIGTERM is received.
	waitForSignal()

	// Cancel the root context to stop the outbox publisher goroutine.
	cancel()

	// Gracefully drain in-flight HTTP requests within the configured timeout.
	ctxTimeout, cancel2 := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel2()
	_ = srv.Shutdown(ctxTimeout)
	log.Printf("shutdown complete")
}

// waitForSignal blocks the calling goroutine until an OS interrupt
// (SIGINT) or termination (SIGTERM) signal is received.
func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

// hostname returns the machine's hostname, used as the outbox publisher's
// worker ID for lock visibility. Falls back to "api-1" if unavailable.
func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "api-1"
	}
	return h
}
