package entity

import "net/http"

type ErrorKind string

const (
	KindUnknown            ErrorKind = "unknown"
	KindNotFound           ErrorKind = "not_found"
	KindAlreadyExists      ErrorKind = "already_exists"
	KindInvalidArgument    ErrorKind = "invalid_argument"
	KindFailedPrecondition ErrorKind = "failed_precondition"
	KindUnauthorized       ErrorKind = "unauthorized"
	KindForbidden          ErrorKind = "forbidden"
)

type AppError struct {
	Kind    ErrorKind
	Message string
}

func (e *AppError) Error() string { return e.Message }

func (e *AppError) HTTPStatus() int {
	switch e.Kind {
	case KindNotFound:
		return http.StatusNotFound
	case KindAlreadyExists, KindFailedPrecondition:
		return http.StatusConflict
	case KindInvalidArgument:
		return http.StatusBadRequest
	case KindUnauthorized:
		return http.StatusUnauthorized
	case KindForbidden:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

func KindFromHTTPStatus(status int) ErrorKind {
	switch status {
	case http.StatusBadRequest:
		return KindInvalidArgument
	case http.StatusNotFound:
		return KindNotFound
	case http.StatusConflict:
		return KindAlreadyExists
	case http.StatusUnauthorized:
		return KindUnauthorized
	case http.StatusForbidden:
		return KindForbidden
	default:
		return KindUnknown
	}
}

func NewAppError(kind ErrorKind, message string) *AppError {
	return &AppError{Kind: kind, Message: message}
}

var (
	ErrNotFound      = NewAppError(KindNotFound, "not found")
	ErrAlreadyExists = NewAppError(KindAlreadyExists, "already exists")
	ErrInvalidInput  = NewAppError(KindInvalidArgument, "invalid input")
	ErrForbidden     = NewAppError(KindForbidden, "forbidden")
	ErrUnauthorized  = NewAppError(KindUnauthorized, "unauthorized")
)
