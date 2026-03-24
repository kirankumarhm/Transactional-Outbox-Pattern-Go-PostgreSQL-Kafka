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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg.DB)
	if err != nil {
		log.Fatalf("db init failed: %v", err)
	}
	defer pool.Close()

	q := sqlc.New(pool)

	prod, err := outbox.NewKafkaProducer(cfg.Kafka)
	if err != nil {
		log.Fatalf("kafka producer init failed: %v", err)
	}
	defer prod.Close()

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

	r := gin.New()
	r.Use(gin.Recovery())

	h := &httpapi.OrdersHandler{Pool: pool, Q: q}
	h.Register(r)

	srv := &http.Server{
		Addr:              cfg.Server.Port,
		Handler:           r,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
	}

	go func() {
		log.Printf("HTTP listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	waitForSignal()
	cancel()

	ctxTimeout, cancel2 := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel2()
	_ = srv.Shutdown(ctxTimeout)
	log.Printf("shutdown complete")
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "api-1"
	}
	return h
}
