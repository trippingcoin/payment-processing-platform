package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"triple-p/cmd/payment-service/repo"
	"triple-p/cmd/payment-service/service"
	"triple-p/config"
	"triple-p/internal/eventconsumer"
	"triple-p/internal/paymentconsumer"
	"triple-p/utils/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("payment consumer stopped; offset not acknowledged")
	}
}

func run() error {
	var cfg struct {
		config.Config
		Brokers []string      `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
		Topic   string        `env:"KAFKA_FRAUD_TOPIC" envDefault:"fraud.events.v1"`
		Group   string        `env:"PAYMENT_CONSUMER_GROUP" envDefault:"payment-fraud-decisions-v1"`
		Timeout time.Duration `env:"PAYMENT_CONSUMER_TIMEOUT" envDefault:"10s"`
	}
	if err := env.Load(&cfg); err != nil {
		return err
	}
	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Timeout <= 0 {
		return fmt.Errorf("invalid payment consumer configuration")
	}
	for _, b := range cfg.Brokers {
		if strings.TrimSpace(b) == "" {
			return fmt.Errorf("empty Kafka broker")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := repo.Open(ctx, cfg.Config)
	if err != nil {
		return err
	}
	defer store.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.Brokers, Topic: cfg.Topic, GroupID: cfg.Group,
		StartOffset: kafka.FirstOffset, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second})
	defer reader.Close()
	log.Info().Str("topic", cfg.Topic).Str("group", cfg.Group).Msg("payment consumer started")
	err = eventconsumer.Consume(ctx, reader, paymentconsumer.Handler{Payments: service.New(store)}, cfg.Timeout)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
