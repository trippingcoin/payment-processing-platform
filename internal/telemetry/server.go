package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Config struct {
	Addr string `env:"METRICS_ADDR" envDefault:"127.0.0.1:9090"`
}

// Serve fails startup on bind errors, cancels work on listener failure, and
// shuts down the operational listener before its database dependencies close.
func Serve(ctx context.Context, addr string, handler http.Handler, work func(context.Context) error) error {
	if addr == "" {
		return fmt.Errorf("METRICS_ADDR must not be empty")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen metrics: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			cancel()
		}
		done <- err
	}()
	workErr := work(ctx)
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	serveErr := <-done
	if !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve metrics: %w", serveErr)
	}
	return errors.Join(workErr, shutdownErr)
}
