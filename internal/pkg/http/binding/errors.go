package binding

import (
	"errors"
	"net/http"

	"github.com/go-playground/validator/v10"
)

// Ошибки binding.
//
//goland:noinspection GoErrorStringFormat
var (
	// ErrMalformedSource — некорректный формат данных (JSON syntax error и т.д.)
	ErrMalformedSource = errors.New("Binding: Malformed HTTP source")

	// ErrValidationFailed — ошибка валидации (используется для errors.Is)
	ErrValidationFailed = (*validationFailedError)(nil)
)

// ScanAndValidateJSON парсит JSON body и валидирует результат.
func ScanAndValidateJSON(r *http.Request, to any) error {
	return scanAndValidate(r, to, bJSON)
}

// ScanAndValidateQuery парсит query параметры и валидирует результат.
func ScanAndValidateQuery(r *http.Request, to any) error {
	return scanAndValidate(r, to, bQuery)
}

// OnlyValidate только валидирует структуру без парсинга.
func OnlyValidate(to any) error {
	return scanAndValidate(nil, to, bOnlyValidate)
}

func scanAndValidate(r *http.Request, to any, b Binding) error {
	err := b.Bind(r, to)
	if err == nil {
		return nil
	}

	var validationErr validator.ValidationErrors
	if errors.As(err, &validationErr) {
		return &validationFailedError{validationErr}
	}

	return ErrMalformedSource
}

// ValidationDetails извлекает список текстов ошибок валидации из err.
// Возвращает nil, если err не является ошибкой валидации.
//
// Используйте в обработчике ошибок для построения JSON-ответа 400.
func ValidationDetails(err error) []string {
	var errList validator.ValidationErrors
	var typedErr *validationFailedError

	switch {
	case errors.As(err, &errList):
		// errList уже заполнен
	case errors.As(err, &typedErr):
		errList = typedErr.originalErr
	default:
		return nil
	}

	details := make([]string, len(errList))
	for i := range errList {
		details[i] = errList[i].Error()
	}
	return details
}

// validationFailedError — обёртка над validator.ValidationErrors.
type validationFailedError struct {
	originalErr validator.ValidationErrors
}

func (e *validationFailedError) Error() string {
	return "Binding: Validation failed"
}

func (e *validationFailedError) Is(other error) bool {
	var errValidationFailed *validationFailedError
	return errors.As(other, &errValidationFailed)
}
