// Package eventerror classifies delivery failures without deciding their disposition.
package eventerror

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

type Kind string

const (
	None      Kind = "none"
	Transient Kind = "transient"
	Permanent Kind = "permanent"
	Canceled  Kind = "canceled"
	Unknown   Kind = "unknown"
)

type Classification struct {
	Kind Kind
	Code string
}
type classifiedError struct {
	classification Classification
	cause          error
}

func (e *classifiedError) Error() string { return e.cause.Error() }
func (e *classifiedError) Unwrap() error { return e.cause }

// Mark preserves errors.Is/As and does not turn a nil result into a failure.
func Mark(kind Kind, code string, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{classification: Classification{kind, code}, cause: err}
}

// Classify uses error types and codes, never message matching. Unknown failures
// need investigation; they must not be silently treated as poison messages.
// In particular, SQL schema/auth/constraint bugs are not evidence of bad input.
func Classify(err error) Classification {
	if err == nil {
		return Classification{None, ""}
	}
	if errors.Is(err, context.Canceled) {
		return Classification{Canceled, "context_canceled"}
	}
	var marked *classifiedError
	if errors.As(err, &marked) {
		return marked.classification
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Classification{Transient, "timeout"}
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		if strings.HasPrefix(pg.Code, "08") {
			return Classification{Transient, "postgres_connection"}
		}
		switch pg.Code {
		case "40001", "40P01":
			return Classification{Transient, "postgres_transaction_conflict"}
		case "55P03":
			return Classification{Transient, "postgres_lock_unavailable"}
		case "57014":
			return Classification{Transient, "postgres_query_canceled"}
		case "57P01", "57P02", "57P03", "53300":
			return Classification{Transient, "postgres_unavailable"}
		default:
			return Classification{Unknown, "postgres_unclassified"}
		}
	}
	var ke kafka.Error
	if errors.As(err, &ke) {
		if ke.Timeout() || ke.Temporary() {
			return Classification{Transient, "kafka_temporary"}
		}
		return Classification{Unknown, "kafka_unclassified"}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return Classification{Transient, "network_timeout"}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return Classification{Transient, "connection_unavailable"}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsTemporary {
		return Classification{Transient, "dns_temporary"}
	}
	return Classification{Unknown, "unclassified"}
}
