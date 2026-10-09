package httpapi

import (
	"encoding/json"
	"net/http"
	"time"
	"triple-p/cmd/payment-service/repo"
)

type paymentDTO struct {
	ID          string             `json:"id"`
	ClientID    string             `json:"client_id"`
	AccountID   string             `json:"account_id"`
	Amount      int64              `json:"amount_minor"`
	Currency    string             `json:"currency"`
	Status      repo.PaymentStatus `json:"status"`
	FailureCode *string            `json:"failure_code,omitempty"`
	Description *string            `json:"description,omitempty"`
	Version     int64              `json:"version"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

type paymentResponse struct {
	Data paymentDTO `json:"data"`
}

type accountDTO struct {
	ID               string    `json:"id"`
	UserID           string    `json:"user_id"`
	Currency         string    `json:"currency"`
	AvailableBalance int64     `json:"available_balance"`
	Version          int64     `json:"version"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type accountResponse struct {
	Data accountDTO `json:"data"`
}

type transactionDTO struct {
	ID           string               `json:"id"`
	AccountID    string               `json:"account_id"`
	PaymentID    *string              `json:"payment_id"`
	RefundID     *string              `json:"refund_id"`
	OperationID  string               `json:"operation_id"`
	EntryType    repo.LedgerEntryType `json:"entry_type"`
	Amount       int64                `json:"amount_minor"`
	BalanceAfter int64                `json:"balance_after"`
	CreatedAt    time.Time            `json:"created_at"`
}

type transactionListResponse struct {
	Data       []transactionDTO `json:"data"`
	NextCursor *string          `json:"next_cursor"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func paymentFromRepo(payment repo.Payment) paymentDTO {
	return paymentDTO{
		ID: payment.ID, ClientID: payment.ClientID, AccountID: payment.AccountID,
		Amount: payment.Amount, Currency: payment.Currency, Status: payment.Status,
		FailureCode: payment.FailureCode, Description: payment.Description,
		Version: payment.Version, CreatedAt: payment.CreatedAt, UpdatedAt: payment.UpdatedAt,
	}
}

func accountFromRepo(account repo.Account) accountDTO {
	return accountDTO{
		ID: account.ID, UserID: account.UserID, Currency: account.Currency,
		AvailableBalance: account.AvailableBalance, Version: account.Version,
		CreatedAt: account.CreatedAt, UpdatedAt: account.UpdatedAt,
	}
}

func transactionFromRepo(entry repo.LedgerEntry) transactionDTO {
	return transactionDTO{
		ID: entry.ID, AccountID: entry.AccountID, PaymentID: entry.PaymentID,
		RefundID: entry.RefundID, OperationID: entry.OperationID, EntryType: entry.EntryType,
		Amount: entry.Amount, BalanceAfter: entry.BalanceAfter, CreatedAt: entry.CreatedAt,
	}
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeError(writer http.ResponseWriter, request *http.Request, status int, code, message string) {
	writeJSON(writer, status, errorResponse{Error: errorBody{
		Code: code, Message: message, RequestID: requestID(request),
	}})
}
