package rprocessor

import (
	"github.com/gorilla/mux"
)

// registerV1Routes регистрирует API v1 routes.
// Шаблон не имеет бизнес-маршрутов: добавляйте их по мере появления handlers.
func registerV1Routes(r *mux.Router) {
	_ = r.PathPrefix("/v1").Subrouter()
}
