package monitor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Lagwick/worker-service/internal/app/config/section"
	"github.com/Lagwick/worker-service/internal/app/processor"
	"github.com/Lagwick/worker-service/internal/app/util"
	"github.com/Lagwick/worker-service/internal/pkg/constant"
)

const (
	initTimeout     = 5 * time.Second
	shutdownTimeout = 5 * time.Second
)

type (
	// openTelemetryProc инициализирует пакеты OpenTelemetry и на завершении
	// приложения сбрасывает накопленные спаны и закрывает соединение.
	openTelemetryProc struct {
		traceProvider *sdktrace.TracerProvider
		conn          *grpc.ClientConn
	}

	openTelemetryErrorHandler struct{}
)

func NewOpenTelemetryController(
	ctx context.Context, env string,
	cfg section.MonitorOpenTelemetry,
) (processor.Processor, error) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()

	var p openTelemetryProc

	// Ресурс: имя сервиса + окружение отдельным атрибутом.

	attrs := []attribute.KeyValue{semconv.ServiceName(constant.AppName)}
	if env != "" {
		attrs = append(attrs, semconv.DeploymentEnvironment(strings.ToLower(env)))
	}

	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	// Соединение с коллектором. grpc.NewClient ленивый и игнорирует WithBlock,
	// поэтому fail-fast делаем явно: дожидаемся connectivity.Ready.

	conn, err := grpc.NewClient(cfg.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for Jaeger: %w", err)
	}
	if err = waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect to Jaeger at %s: %w", cfg.Address, err)
	}
	p.conn = conn

	// Экспортёр спанов поверх готового соединения.

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	// TracerProvider: батч-процессор + сэмплер (ratio зажат в [0, 1]).

	cfg.SampleRatio = min(1, max(0, cfg.SampleRatio))

	bsp := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithExportTimeout(cfg.ExportTimeout),
		sdktrace.WithBatchTimeout(cfg.SendBatchTimeout),
		//nolint:gosec // G115: размеры батча/очереди берутся из конфига, переполнение нереалистично
		sdktrace.WithMaxExportBatchSize(int(cfg.MaxBatchSize)),
		//nolint:gosec // G115: размеры батча/очереди берутся из конфига, переполнение нереалистично
		sdktrace.WithMaxQueueSize(int(cfg.MaxQueueSize)),
	)
	p.traceProvider = sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRatio)),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// Глобальные настройки: провайдер, propagator (W3C), обработчик ошибок.

	otel.SetTracerProvider(p.traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otel.SetErrorHandler(openTelemetryErrorHandler{})

	log.Info().Str("service", constant.AppName).Str("environment", env).
		Msg("OpenTelemetry has been initialized")
	return &p, nil
}

// waitForReady дожидается перехода соединения в connectivity.Ready либо отмены ctx.
func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (p *openTelemetryProc) StartAsync(ctx context.Context, wg *sync.WaitGroup) {
	processor.WatchForShutdown(ctx, wg, util.CloserFunc(p.shutdown))
}

func (p *openTelemetryProc) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := p.traceProvider.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to shutdown trace provider")
	}
	if err := p.conn.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close Jaeger gRPC connection")
	}
	return nil
}

func (openTelemetryErrorHandler) Handle(err error) {
	log.Error().Err(err).Msg("OpenTelemetry error")
}
