package rprocessor

import (
	"net/http"
	"net/http/pprof"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registerHealthRoutes регистрирует служебные endpoints: health и Prometheus-метрики.
func registerHealthRoutes(r *mux.Router) {
	reg(r, http.MethodGet, "/health", http.HandlerFunc(healthHandler))
	reg(r, http.MethodGet, "/metrics", promhttp.Handler())
}

// registerPprofRoutes регистрирует профилировщик net/http/pprof.
func registerPprofRoutes(r *mux.Router) {
	r.HandleFunc("/debug/pprof", pprofIndex)
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func pprofIndex(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/debug/pprof/", http.StatusMovedPermanently)
}

// healthHandler — простой health check handler.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// notFoundHandler — обработчик 404 Not Found.
func notFoundHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error":"not found"}`))
}

// methodNotAllowedHandler — обработчик 405 Method Not Allowed.
func methodNotAllowedHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = w.Write([]byte(`{"error":"method not allowed"}`))
}
