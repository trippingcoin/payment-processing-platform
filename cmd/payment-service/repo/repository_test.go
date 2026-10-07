package repo

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMapPostgresError(t *testing.T) {
	tests := []struct {
		name string
		code string
		want error
	}{
		{name: "unique violation", code: "23505", want: ErrAlreadyExists},
		{name: "check violation", code: "23514", want: ErrInvalidState},
		{name: "foreign key violation", code: "23503", want: ErrInvalidState},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapPostgresError(&pgconn.PgError{Code: tt.code})
			if !errors.Is(err, tt.want) {
				t.Fatalf("mapPostgresError() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAccountWritesRejectNonPositiveAmount(t *testing.T) {
	queries := &Queries{}

	if _, err := queries.DebitAccount(context.Background(), "account", "KZT", 0, 0); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("DebitAccount() error = %v, want %v", err, ErrInvalidState)
	}
	if _, err := queries.CreditAccount(context.Background(), "account", "KZT", -1, 0); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("CreditAccount() error = %v, want %v", err, ErrInvalidState)
	}
}
