package httph

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

type Middleware = func(http.Handler) http.Handler

type _ContextKeyError struct{}

type _ContextValueError struct {
	err        error
	statusCode int
}

// ErrorPrepare подготавливает request для хранения ошибки в контексте.
func ErrorPrepare(r *http.Request) *http.Request {
	return r.WithContext(errorPrepare(r.Context()))
}

// ErrorGet возвращает ошибку из контекста request.
func ErrorGet(r *http.Request) error {
	return errorGet(r.Context())
}

// ErrorApply устанавливает ошибку в контекст request.
func ErrorApply(r *http.Request, err error) {
	errorApply(r.Context(), err)
}

// ErrorApplyStatusCode сохраняет HTTP-статус ответа в контекст request.
func ErrorApplyStatusCode(r *http.Request, statusCode int) {
	errorApplyStatusCode(r.Context(), statusCode)
}

// ErrorGetStatusCode возвращает HTTP-статус ответа из контекста request.
func ErrorGetStatusCode(r *http.Request) int {
	return errorGetStatusCode(r.Context())
}

// NewErrorMiddleware создаёт middleware, который подготавливает контекст для хранения ошибок.
// Должен быть добавлен первым в цепочку middleware.
func NewErrorMiddleware() Middleware {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ServeHTTP(w, ErrorPrepare(r))
		})
	}
}

////////////////////////////////////////////////////////////////////////////////
///// PRIVATE METHODS //////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

func errorPrepare(ctx context.Context) context.Context {
	return context.WithValue(ctx, _ContextKeyError{}, new(_ContextValueError))
}

func errorGet(ctx context.Context) error {
	errV, _ := ctx.Value(_ContextKeyError{}).(*_ContextValueError)
	if errV != nil {
		return errV.err
	}
	return nil
}

func errorApply(ctx context.Context, err error) {
	errV, _ := ctx.Value(_ContextKeyError{}).(*_ContextValueError)
	if errV != nil {
		errV.err = err
	}

	// Фиксируем ошибку на активном спане: добавляется событие exception
	// с текстом ошибки, видимое в трейсе. Если трейсинг выключен, спан
	// no-op и вызов ничего не делает.
	if err != nil {
		trace.SpanFromContext(ctx).RecordError(err)
	}
}

func errorApplyStatusCode(ctx context.Context, statusCode int) {
	errV, _ := ctx.Value(_ContextKeyError{}).(*_ContextValueError)
	if errV != nil {
		errV.statusCode = statusCode
	}
}

func errorGetStatusCode(ctx context.Context) int {
	errV, _ := ctx.Value(_ContextKeyError{}).(*_ContextValueError)
	if errV != nil {
		return errV.statusCode
	}
	return 0
}
