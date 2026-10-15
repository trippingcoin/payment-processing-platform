package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"triple-p/internal/dlqreplay"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv("KAFKA_BROKERS"), os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, envBrokers string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("dlq-replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Replay one DLQ record. Default: read-only dry run. Execute requires --dlq-id from dry run.\nNo offsets are committed; no records are deleted. Fix the cause and inspect Inbox before replay.")
		fs.PrintDefaults()
	}
	if envBrokers == "" {
		envBrokers = "localhost:19092"
	}
	brokers := fs.String("brokers", envBrokers, "comma-separated Kafka brokers (or KAFKA_BROKERS)")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout per Kafka operation")
	var r dlqreplay.Request
	fs.StringVar(&r.DLQTopic, "dlq-topic", "", "DLQ topic to read")
	fs.IntVar(&r.Partition, "partition", -1, "DLQ partition (required)")
	fs.Int64Var(&r.Offset, "offset", -1, "exact DLQ offset (required)")
	fs.StringVar(&r.SourceTopic, "source-topic", "", "expected original topic; must match DLQ record")
	fs.StringVar(&r.ConsumerGroup, "consumer-group", "", "expected failed consumer group; not joined")
	fs.StringVar(&r.DLQID, "dlq-id", "", "expected dlq_id; required with --execute")
	fs.BoolVar(&r.Execute, "execute", false, "publish after validation (otherwise dry run)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments; use named flags")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	addresses := strings.Split(*brokers, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}
	store, err := dlqreplay.NewKafkaStore(addresses, *timeout)
	if err != nil {
		return err
	}
	result, err := dlqreplay.Run(ctx, store, r)
	if result.Status != "" {
		if outputErr := json.NewEncoder(stdout).Encode(result); outputErr != nil {
			return errors.Join(err, fmt.Errorf("write result (publication status may already be final): %w", outputErr))
		}
	}
	return err
}
