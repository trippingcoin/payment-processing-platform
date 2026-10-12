package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"triple-p/cmd/payment-service/repo"
	"triple-p/config"
	"triple-p/internal/outbox"
	"triple-p/utils/env"

	"github.com/rs/zerolog/log"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("outbox worker stopped")
	}
}

func run() error {
	var cfg struct {
		config.Config
		Outbox      outbox.Config
		Brokers     []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
		Topic       string   `env:"KAFKA_PAYMENTS_TOPIC" envDefault:"payments.events.v1"`
		OutputTopic string   `env:"OUTBOX_TOPIC"`
	}
	if err := env.Load(&cfg); err != nil {
		return err
	}
	if cfg.OutputTopic != "" {
		cfg.Topic = cfg.OutputTopic
	}

	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.Topic) == "" {
		return fmt.Errorf("Kafka brokers and topic are required")
	}

	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
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
	publisher := outbox.NewKafkaPublisher(cfg.Brokers, cfg.Topic, cfg.Outbox.PublishTimeout)
	defer publisher.Close()
	worker, err := outbox.New(outbox.PostgresTransactions(store), publisher, cfg.Outbox)
	if err != nil {
		return err
	}
	log.Info().Str("topic", cfg.Topic).Msg("outbox worker started")
	err = worker.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
