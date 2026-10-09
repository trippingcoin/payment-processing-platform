package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"
	"triple-p/cmd/payment-service/httpapi"
	"triple-p/cmd/payment-service/repo"
	"triple-p/cmd/payment-service/service"
	"triple-p/config"
	"triple-p/utils/env"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("payment service stopped")
	}
}

func run() error {
	var cfg config.Config
	if err := env.Load(&cfg); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := repo.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	paymentService := service.New(store)
	handler := httpapi.New(paymentService, store, cfg.InternalToken)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		log.Info().Int("port", cfg.Port).Str("version", cfg.Version).Msg("payment service started")
		serverError <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	return nil
}
