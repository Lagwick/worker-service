package rprocessor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"github.com/Lagwick/worker-service/internal/app/config/section"
	"github.com/Lagwick/worker-service/internal/app/processor"
	"github.com/Lagwick/worker-service/internal/app/util"
	"github.com/Lagwick/worker-service/internal/pkg/http/httph"
	"github.com/Lagwick/worker-service/internal/pkg/http/mzerolog"
)

// httpProc — HTTP сервер, реализующий интерфейс processor.Processor.
type httpProc struct {
	server http.Server
	addr   string
}

// NewHTTP создаёт HTTP процессор со служебными маршрутами (health и пр.).
func NewHTTP(
	middlewares []httph.Middleware,
	cfg section.ProcessorWebServer,
) processor.Processor {
	// 1. Создаём роутер
	r := mux.NewRouter()
	r.StrictSlash(true) // редирект trailing slashes

	// 2. Внешние middleware (OpenTelemetry и др.) — снаружи, чтобы span попал
	//    в контекст запроса до логирования (trace_id в логах).
	r.Use(middlewaresToGorilla(middlewares)...)

	// 3. Базовые middleware
	r.Use(
		httph.NewErrorMiddleware(), // error context
		mzerolog.NewMiddleware(mzerolog.WithSkipper(util.IsFilteredHttpRoute)), // логирование запросов
	)

	// 4. Регистрируем routes
	registerHealthRoutes(r)
	registerPprofRoutes(r)
	registerV1Routes(r)

	// 5. Обработка 404/405
	r.NotFoundHandler = http.HandlerFunc(notFoundHandler)
	r.MethodNotAllowedHandler = http.HandlerFunc(methodNotAllowedHandler)

	// 6. Логируем зарегистрированные routes (для отладки)
	_ = r.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		path, _ := route.GetPathTemplate()
		methods, _ := route.GetMethods()
		if path != "" && len(methods) > 0 {
			log.Debug().Str("path", path).Strs("methods", methods).Msg("Registered route")
		}
		return nil
	})

	// 7. Создаём процессор
	p := &httpProc{
		addr: fmt.Sprintf(":%d", cfg.ListenPort),
	}
	p.server.Handler = r

	return p
}

// StartAsync запускает HTTP сервер асинхронно.
// При отмене контекста сервер корректно завершается (graceful shutdown).
func (p *httpProc) StartAsync(ctx context.Context, wg *sync.WaitGroup) {
	// 1. Создаём listener с поддержкой контекста
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", p.addr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", p.addr).Msg("Не удалось запустить HTTP listener")
		return
	}

	log.Info().Str("addr", p.addr).Msg("HTTP сервер запущен")

	// 2. Запускаем сервер в отдельной горутине (блокирующая операция)
	go p.serve(l)

	// 3. Регистрируем shutdown handlers
	// ВАЖНО: НЕ используем `go` перед WatchForShutdown, т.к. функция сама запускает горутину
	// При отмене ctx закроем listener
	processor.WatchForShutdown(ctx, wg, util.CloserFunc(l.Close))

	// При отмене ctx вызовем Shutdown с таймаутом 5 секунд
	processor.WatchForShutdown(ctx, wg, util.NewCloserContextFunc(
		p.server.Shutdown, context.Background(), 5*time.Second,
	))
}

func (p *httpProc) serve(l net.Listener) {
	_ = p.server.Serve(l) // блокирует горутину до закрытия listener
}
