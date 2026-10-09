package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"triple-p/cmd/payment-service/repo"
	"triple-p/cmd/payment-service/service"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const maxRequestBodyBytes = 1 << 20

type PaymentService interface {
	CreatePayment(context.Context, service.CreatePaymentCommand) (service.CreatePaymentResult, error)
	GetPayment(context.Context, string, string) (repo.Payment, error)
	ProcessPayment(context.Context, service.ProcessPaymentCommand) (repo.Payment, error)
	GetAccount(context.Context, string, string) (repo.Account, error)
	ListTransactions(context.Context, string, string, string, int) (service.TransactionPage, error)
}

type ReadinessChecker interface {
	Ping(context.Context) error
}

type Handler struct {
	service       PaymentService
	readiness     ReadinessChecker
	internalToken string
	mux           *http.ServeMux
}

func New(service PaymentService, readiness ReadinessChecker, internalToken string) *Handler {
	handler := &Handler{
		service:       service,
		readiness:     readiness,
		internalToken: internalToken,
		mux:           http.NewServeMux(),
	}
	handler.routes()
	return handler
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	requestID := strings.TrimSpace(request.Header.Get("X-Request-ID"))
	if requestID == "" || len(requestID) > 128 {
		requestID = uuid.NewString()
	}
	writer.Header().Set("X-Request-ID", requestID)

	ctx := context.WithValue(request.Context(), requestIDContextKey{}, requestID)
	request = request.WithContext(ctx)
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Error().Interface("panic", recovered).Str("request_id", requestID).Msg("http handler panic")
			writeError(writer, request, http.StatusInternalServerError, "internal_error", "Internal server error")
		}
	}()
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) routes() {
	h.mux.HandleFunc("GET /health/live", h.liveness)
	h.mux.HandleFunc("GET /health/ready", h.readinessCheck)
	h.mux.HandleFunc("POST /v1/payments", h.createPayment)
	h.mux.HandleFunc("GET /v1/payments/{payment_id}", h.getPayment)
	h.mux.HandleFunc("GET /v1/accounts/{account_id}", h.getAccount)
	h.mux.HandleFunc("GET /v1/accounts/{account_id}/transactions", h.listTransactions)
	h.mux.HandleFunc("POST /internal/v1/payments/{payment_id}/process", h.processPayment)
}

func (h *Handler) liveness(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) readinessCheck(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), time.Second)
	defer cancel()
	if err := h.readiness.Ping(ctx); err != nil {
		writeError(writer, request, http.StatusServiceUnavailable, "service_unavailable", "Database is unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

type createPaymentRequest struct {
	AccountID   string `json:"account_id"`
	Amount      int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Description string `json:"description"`
}

func (h *Handler) createPayment(writer http.ResponseWriter, request *http.Request) {
	var body createPaymentRequest
	if err := decodeJSON(writer, request, &body); err != nil {
		writeError(writer, request, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	result, err := h.service.CreatePayment(request.Context(), service.CreatePaymentCommand{
		ClientID:       request.Header.Get("X-Client-ID"),
		AccountID:      body.AccountID,
		Amount:         body.Amount,
		Currency:       body.Currency,
		Description:    body.Description,
		IdempotencyKey: request.Header.Get("Idempotency-Key"),
		CorrelationID:  requestID(request),
	})
	if err != nil {
		h.writeServiceError(writer, request, err)
		return
	}
	writer.Header().Set("Idempotent-Replayed", strconv.FormatBool(result.Replayed))
	writeJSON(writer, http.StatusAccepted, paymentResponse{Data: paymentFromRepo(result.Payment)})
}

func (h *Handler) getPayment(writer http.ResponseWriter, request *http.Request) {
	payment, err := h.service.GetPayment(
		request.Context(),
		request.Header.Get("X-Client-ID"),
		request.PathValue("payment_id"),
	)
	if err != nil {
		h.writeServiceError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, paymentResponse{Data: paymentFromRepo(payment)})
}

func (h *Handler) processPayment(writer http.ResponseWriter, request *http.Request) {
	if !secureEqual(h.internalToken, request.Header.Get("X-Internal-Token")) {
		writeError(writer, request, http.StatusUnauthorized, "unauthorized", "Invalid internal token")
		return
	}
	payment, err := h.service.ProcessPayment(request.Context(), service.ProcessPaymentCommand{
		PaymentID:     request.PathValue("payment_id"),
		CorrelationID: requestID(request),
	})
	if err != nil {
		h.writeServiceError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, paymentResponse{Data: paymentFromRepo(payment)})
}

func (h *Handler) getAccount(writer http.ResponseWriter, request *http.Request) {
	account, err := h.service.GetAccount(
		request.Context(),
		request.Header.Get("X-Client-ID"),
		request.PathValue("account_id"),
	)
	if err != nil {
		h.writeServiceError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, accountResponse{Data: accountFromRepo(account)})
}

func (h *Handler) listTransactions(writer http.ResponseWriter, request *http.Request) {
	limit := 50
	if value := request.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			writeError(writer, request, http.StatusBadRequest, "invalid_request", "limit must be an integer")
			return
		}
		limit = parsed
	}
	page, err := h.service.ListTransactions(
		request.Context(),
		request.Header.Get("X-Client-ID"),
		request.PathValue("account_id"),
		request.URL.Query().Get("cursor"),
		limit,
	)
	if err != nil {
		h.writeServiceError(writer, request, err)
		return
	}

	entries := make([]transactionDTO, 0, len(page.Entries))
	for _, entry := range page.Entries {
		entries = append(entries, transactionFromRepo(entry))
	}
	var nextCursor *string
	if page.NextCursor != "" {
		nextCursor = &page.NextCursor
	}
	writeJSON(writer, http.StatusOK, transactionListResponse{Data: entries, NextCursor: nextCursor})
}

func (h *Handler) writeServiceError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		writeError(writer, request, http.StatusUnprocessableEntity, "validation_error", err.Error())
	case errors.Is(err, service.ErrPaymentNotFound), errors.Is(err, service.ErrAccountNotFound):
		writeError(writer, request, http.StatusNotFound, "not_found", "Resource not found")
	case errors.Is(err, service.ErrCurrencyMismatch):
		writeError(writer, request, http.StatusUnprocessableEntity, "currency_mismatch", "Account currency does not match payment currency")
	case errors.Is(err, service.ErrIdempotencyConflict):
		writeError(writer, request, http.StatusConflict, "idempotency_conflict", "Idempotency key was used with another request")
	case errors.Is(err, service.ErrProcessingConflict), errors.Is(err, repo.ErrOptimisticLock), errors.Is(err, repo.ErrInvalidState):
		writeError(writer, request, http.StatusConflict, "state_conflict", "Resource state changed; retry the request")
	default:
		log.Error().Err(err).Str("request_id", requestID(request)).Msg("request failed")
		writeError(writer, request, http.StatusInternalServerError, "internal_error", "Internal server error")
	}
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func secureEqual(expected, actual string) bool {
	if expected == "" || len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

type requestIDContextKey struct{}

func requestID(request *http.Request) string {
	value, _ := request.Context().Value(requestIDContextKey{}).(string)
	return value
}
