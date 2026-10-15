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
	"triple-p/internal/telemetry"
	"triple-p/utils/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Fields(eventconsumer.ErrorFields(err)).Err(err).Msg("payment consumer stopped; offset not acknowledged")
	}
}

func run() error {
	var cfg struct {
		config.Config
		Retry      eventconsumer.RetryConfig
		DLQTopic   string        `env:"PAYMENT_DLQ_TOPIC" envDefault:"payment-consumer.dlq.v1"`
		DLQTimeout time.Duration `env:"CONSUMER_DLQ_TIMEOUT" envDefault:"5s"`
		Brokers    []string      `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
		Topic      string        `env:"KAFKA_FRAUD_TOPIC" envDefault:"fraud.events.v1"`
		Group      string        `env:"PAYMENT_CONSUMER_GROUP" envDefault:"payment-fraud-decisions-v1"`
		Timeout    time.Duration `env:"PAYMENT_CONSUMER_TIMEOUT" envDefault:"10s"`
	}
	if err := env.Load(&cfg); err != nil {
		return err
	}
	if err := cfg.Retry.Validate(); err != nil {
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
	dlq, err := eventconsumer.NewKafkaDeadLetterPublisher(cfg.Brokers, cfg.Topic, cfg.DLQTopic, cfg.DLQTimeout)
	if err != nil {
		return err
	}
	defer dlq.Close()
	store, err := repo.Open(ctx, cfg.Config)
	if err != nil {
		return err
	}
	defer store.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.Brokers, Topic: cfg.Topic, GroupID: cfg.Group,
		StartOffset: kafka.FirstOffset, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second})
	defer reader.Close()
	log.Info().Str("topic", cfg.Topic).Str("group", cfg.Group).Msg("payment consumer started")
	metrics := telemetry.New()
	payments := service.New(store, service.WithPaymentObserver(metrics.PaymentTransition))
	err = telemetry.Serve(ctx, cfg.Metrics.Addr, metrics.Handler(), func(ctx context.Context) error {
		return eventconsumer.ConsumeWithDLQ(ctx, reader, paymentconsumer.Handler{Payments: payments}, cfg.Timeout, cfg.Retry, eventconsumer.DeadLetterSink{Publisher: dlq, ConsumerGroup: cfg.Group, Timeout: cfg.DLQTimeout}, metrics)
	})
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
