package eventconsumer

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"triple-p/internal/eventerror"
)

// MaxAttempts includes the first call. Budgets are per message and stage, in
// memory: restarting the process starts a new budget, not a durable retry count.
type RetryConfig struct {
	MaxAttempts int           `env:"CONSUMER_MAX_ATTEMPTS" envDefault:"3"`
	BaseDelay   time.Duration `env:"CONSUMER_RETRY_BASE" envDefault:"1s"`
	MaxDelay    time.Duration `env:"CONSUMER_RETRY_MAX" envDefault:"30s"`
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second}
}

func (c RetryConfig) Validate() error {
	if c.MaxAttempts < 1 || c.BaseDelay <= 0 || c.MaxDelay < c.BaseDelay {
		return fmt.Errorf("consumer retry requires max attempts >= 1 and 0 < base delay <= max delay")
	}
	return nil
}

// delay uses capped exponential backoff with equal jitter in [cap/2, cap].
// Saturating multiplication avoids overflow even with large configurations.
func (c RetryConfig) delay(failedAttempt int, random func(int64) int64) time.Duration {
	cap := c.BaseDelay
	for n := 1; n < failedAttempt && cap < c.MaxDelay; n++ {
		if cap > c.MaxDelay/2 {
			cap = c.MaxDelay
		} else {
			cap *= 2
		}
	}
	lower := max(time.Nanosecond, cap/2)
	return lower + time.Duration(random(int64(cap-lower)+1))
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type retryRunner struct {
	config RetryConfig
	wait   func(context.Context, time.Duration) error
	random func(int64) int64
}

func newRetryRunner(config RetryConfig) retryRunner {
	return retryRunner{config: config, wait: waitRetry, random: rand.Int64N}
}

func (r retryRunner) run(ctx context.Context, stage string, message kafka.Message, call func(context.Context) error) error {
	failure := &Failure{Stage: stage, Topic: message.Topic, Partition: message.Partition, Offset: message.Offset}
	for {
		if err := ctx.Err(); err != nil {
			failure.Cause = err
			return failure
		}
		failure.Attempts++
		err := call(ctx)
		if err == nil {
			return nil
		}
		failure.Cause = err
		if ctx.Err() != nil {
			failure.Cause = ctx.Err()
			return failure
		}
		if eventerror.Classify(err).Kind != eventerror.Transient {
			return failure
		}
		if failure.Attempts >= r.config.MaxAttempts {
			failure.RetryExhausted = true
			return failure
		}
		delay := r.config.delay(failure.Attempts, r.random)
		log.Warn().Fields(ErrorFields(failure)).Dur("retry_delay", delay).Msg("consumer stage will retry")
		if err = r.wait(ctx, delay); err != nil {
			failure.Cause = err
			return failure
		}
	}
}
