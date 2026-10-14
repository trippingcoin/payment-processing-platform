package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"triple-p/internal/eventconsumer"
	"triple-p/internal/fraud"
	"triple-p/utils/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Fields(eventconsumer.ErrorFields(err)).Err(err).Msg("fraud service stopped; offset not acknowledged")
	}
}

func run() error {
	var cfg struct {
		Rules   fraud.Rules
		DSN     string        `env:"FRAUD_DB_DSN" envDefault:"postgres://fraud:fraud@localhost:55433/fraud?sslmode=disable"`
		Brokers []string      `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
		Topic   string        `env:"KAFKA_PAYMENTS_TOPIC" envDefault:"payments.events.v1"`
		Group   string        `env:"FRAUD_CONSUMER_GROUP" envDefault:"fraud-service-v1"`
		Timeout time.Duration `env:"FRAUD_PROCESS_TIMEOUT" envDefault:"10s"`
	}
	if err := env.Load(&cfg); err != nil {
		return err
	}
	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Timeout <= 0 {
		return fmt.Errorf("invalid fraud consumer configuration")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("empty Kafka broker")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	checkCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	err = pool.Ping(checkCtx)
	cancel()
	if err != nil {
		return err
	}
	service, err := fraud.New(fraud.NewPostgresStore(pool), cfg.Rules)
	if err != nil {
		return err
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.Brokers, Topic: cfg.Topic, GroupID: cfg.Group,
		StartOffset: kafka.FirstOffset, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second})
	defer reader.Close()
	log.Info().Str("topic", cfg.Topic).Str("group", cfg.Group).Msg("fraud service started")
	err = fraud.Consume(ctx, reader, service, cfg.Timeout)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
