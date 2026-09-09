package builder

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"

	"github.com/Lagwick/worker-service/internal/app/client/fixer"
	"github.com/Lagwick/worker-service/internal/app/config"
	"github.com/Lagwick/worker-service/internal/app/entity"
	eorder "github.com/Lagwick/worker-service/internal/app/handler/event/order"
	"github.com/Lagwick/worker-service/internal/app/processor"
	eprocessor "github.com/Lagwick/worker-service/internal/app/processor/event"
	rprocessor "github.com/Lagwick/worker-service/internal/app/processor/http"
	mmonitor "github.com/Lagwick/worker-service/internal/app/processor/monitor"
	"github.com/Lagwick/worker-service/internal/app/repository"
	rcredis "github.com/Lagwick/worker-service/internal/app/repository/conn/redis"
	rcurrency "github.com/Lagwick/worker-service/internal/app/repository/currency"
	"github.com/Lagwick/worker-service/internal/app/service"
	scurrency "github.com/Lagwick/worker-service/internal/app/service/currency"
	sdelivery "github.com/Lagwick/worker-service/internal/app/service/delivery"
	"github.com/Lagwick/worker-service/internal/app/util"
	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/Lagwick/worker-service/internal/pkg/broker/codec"
	"github.com/Lagwick/worker-service/internal/pkg/constant"
	"github.com/Lagwick/worker-service/internal/pkg/http/httph"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

// Builder — структура для сборки зависимостей приложения.
// Использует паттерн Builder для последовательной инициализации компонентов.
type Builder struct {
	cCtx      *cli.Context
	ctx       context.Context
	wg        sync.WaitGroup
	err       error
	connRedis *rcredis.Client
	// Внешние клиенты
	clientFixer *fixer.Client

	// Репозитории
	repoCurrencyRate repository.CurrencyRate

	// Сервисы
	currencyService service.Currency
	deliveryService service.Delivery

	// Процессоры
	processors []processor.Processor

	// HTTP middleware (OpenTelemetry, NewRelic, и др.)
	middlewares []httph.Middleware

	// Kafka-клиент (общий producer + фабрика consumer groups)
	kafkaClient *broker.KafkaClient

	busOrderCreated broker.Bus[entity.EventOrderCreated]

	// Шина события order.delivery.calculated (producer)
	busOrderDeliveryCalculated broker.Bus[entity.EventOrderDeliveryCalculated]
}

// NewBuilder создаёт новый Builder и настраивает обработку сигналов OS.
// При получении SIGINT/SIGTERM контекст будет отменён.
func NewBuilder(cCtx *cli.Context) *Builder {
	b := Builder{cCtx: cCtx}
	var cancelFunc func()
	b.ctx, cancelFunc = context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go b.waitForSignal(sig, cancelFunc)

	return &b
}

////////////////////////////////////////////////////////////////////////////////
///// CONFIG ///////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildConfig загружает конфигурацию из .env и переменных окружения.
// Можно передать injectors для модификации конфига после загрузки.
func (b *Builder) BuildConfig(injectors ...func(c *config.Config)) {
	b.buildConfig(config.LoadArgs{}, injectors)
}

func (b *Builder) buildConfig(args config.LoadArgs, injectors []func(c *config.Config)) {
	if b.err != nil {
		return
	}

	// Определяем формат логов из CLI флага
	if b.cCtx != nil && b.cCtx.Bool("no-json") {
		args.EnableSimpleLog = true
	}
	args.Output = os.Stdout

	config.Load(args)

	// Применяем injectors
	for _, injector := range injectors {
		if injector != nil {
			injector(&config.Root)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
///// HANDLERS /////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// TODO: добавить методы BuildHandlerXxx по мере появления handlers.

////////////////////////////////////////////////////////////////////////////////
///// BROKER ///////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildBrokerKafka создаёт общий Kafka-клиент и регистрирует его закрытие на shutdown.
// Шину под конкретное событие (broker.NewBus[Event]) создавайте уже в сервисе.
func (b *Builder) BuildBrokerKafka() {
	b.exec(func(b *Builder) {
		cfg := config.Root.Broker.Kafka

		client, err := broker.NewKafkaClient(broker.KafkaConfig{
			Addresses:     cfg.Addresses,
			ConsumerGroup: cfg.ConsumerGroup,
			ClientID:      cfg.ClientID,
		})
		if err != nil {
			b.err = fmt.Errorf("init kafka client: %w", err)
			return
		}
		b.kafkaClient = client

		b.processors = append(b.processors, processor.ProcessorFunc(func(ctx context.Context, wg *sync.WaitGroup) {
			processor.WatchForShutdown(ctx, wg, util.CloserFunc(client.Close))
		}))
	})
}

func (b *Builder) BuildConsumerOrderCreated() {
	b.exec(func(b *Builder) {
		cfg := config.Root.Broker.Kafka

		topic := cfg.ModelOrder.Created.Topic
		group := broker.Coalesce(
			cfg.ModelOrder.Created.ConsumerGroup,
			cfg.ConsumerGroup,
		)

		busOrderCreated, err := broker.NewBus[entity.EventOrderCreated](
			b.kafkaClient,
			codec.NewCodecJson[entity.EventOrderCreated](),
			topic,
			group,
		)
		if err != nil {
			b.err = fmt.Errorf("init order.created bus: %w", err)
			return
		}

		b.busOrderCreated = busOrderCreated

		h := eorder.NewHandler(
			b.deliveryService,
			b.busOrderDeliveryCalculated,
		)

		b.processors = append(
			b.processors,
			eprocessor.NewOrderCreatedEventsCatcher(
				h,
				b.busOrderCreated,
			),
		)
	}, b.kafkaClient, b.deliveryService, b.busOrderDeliveryCalculated)
}

// BuildBusOrderDeliveryCalculated создаёт publish-шину топика order.delivery.calculated.
func (b *Builder) BuildBusOrderDeliveryCalculated() {
	b.exec(func(b *Builder) {
		cfg := config.Root.Broker.Kafka

		busOrderDeliveryCalculated, err := broker.NewBus[entity.EventOrderDeliveryCalculated](
			b.kafkaClient,
			codec.NewCodecJson[entity.EventOrderDeliveryCalculated](),
			cfg.ModelOrder.DeliveryCalculated.Topic,
			cfg.ConsumerGroup,
		)
		if err != nil {
			b.err = fmt.Errorf("init order.delivery.calculated bus: %w", err)
			return
		}

		b.busOrderDeliveryCalculated = busOrderDeliveryCalculated
	}, b.kafkaClient)
}

////////////////////////////////////////////////////////////////////////////////
///// CLIENTS //////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildClientFixer создаёт HTTP-клиент Fixer API (курсы валют).
func (b *Builder) BuildClientFixer() {
	b.exec(func(b *Builder) {
		b.clientFixer = fixer.NewClient(config.Root.Client.Fixer)
	})
}

////////////////////////////////////////////////////////////////////////////////
///// REPOSITORY CONNECTIONS ///////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildConnRedis открывает подключение к Redis и проверяет его через Ping.
func (b *Builder) BuildConnRedis() {
	b.exec(func(b *Builder) {
		connRedis, err := rcredis.NewClient(
			b.ctx,
			config.Root.Repository.Redis,
		)
		if err != nil {
			b.err = fmt.Errorf("init redis connection: %w", err)
			return
		}

		b.connRedis = connRedis

		b.processors = append(
			b.processors,
			processor.ProcessorFunc(func(ctx context.Context, wg *sync.WaitGroup) {
				processor.WatchForShutdown(
					ctx,
					wg,
					util.CloserFunc(connRedis.Close),
				)
			}),
		)
	})
}

////////////////////////////////////////////////////////////////////////////////
///// REPOSITORIES /////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildRepoCurrencyRate создаёт Redis-кэш курсов валют.
func (b *Builder) BuildRepoCurrencyRate() {
	b.exec(func(b *Builder) {
		b.repoCurrencyRate = rcurrency.NewRepoFromRedis(
			b.connRedis.Client,
			config.Root.Client.Fixer.CacheTTL,
		)
	}, b.connRedis)
}

////////////////////////////////////////////////////////////////////////////////
///// SERVICES /////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildServiceCurrency собирает сервис курсов валют (cache-aside: Redis -> Fixer).
func (b *Builder) BuildServiceCurrency() {
	b.exec(func(b *Builder) {
		b.currencyService = scurrency.NewService(
			b.clientFixer,
			b.repoCurrencyRate,
		)
	}, b.clientFixer, b.repoCurrencyRate)
}

func (b *Builder) BuildServiceDelivery() {
	b.exec(func(b *Builder) {
		b.deliveryService = sdelivery.NewService(
			b.currencyService,
		)
	}, b.currencyService)
}

////////////////////////////////////////////////////////////////////////////////
///// MONITORS /////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildMonitorOpenTelemetry инициализирует TracerProvider (если включён в конфиге)
// и добавляет otelmux-middleware в HTTP-цепочку.
func (b *Builder) BuildMonitorOpenTelemetry() {
	cfg := config.Root.Monitor.OpenTelemetry
	if !cfg.Enabled {
		log.Warn().Msg("OpenTelemetry выключен в конфиге")
		return
	}

	b.exec(func(b *Builder) {
		proc, err := mmonitor.NewOpenTelemetryController(b.ctx, config.Root.Monitor.Environment, cfg)
		if err != nil {
			b.err = fmt.Errorf("init OpenTelemetry: %w", err)
			return
		}
		b.processors = append(b.processors, proc)

		b.middlewares = append(b.middlewares, otelmux.Middleware(
			constant.AppName,
			otelmux.WithFilter(func(r *http.Request) bool {
				return !util.IsFilteredHttpRoute(r)
			}),
		))
	})
}

////////////////////////////////////////////////////////////////////////////////
///// PROCESSORS ///////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// BuildProcHttp создаёт и добавляет HTTP процессор (служебные маршруты: health и пр.).
func (b *Builder) BuildProcHttp() {
	b.exec(func(b *Builder) {
		cfg := config.Root.Processor.WebServer
		proc := rprocessor.NewHTTP(b.middlewares, cfg)
		b.processors = append(b.processors, proc)
	})
}

////////////////////////////////////////////////////////////////////////////////
///// RUN //////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// Run запускает все подготовленные процессоры и ожидает их завершения.
func (b *Builder) Run() {
	if b.err != nil {
		log.Fatal().Err(b.err).Msg("Ошибка при инициализации приложения")
	}

	log.Info().Msg("Приложение инициализировано")
	defer log.Info().Msg("Приложение завершено, до свидания!")

	for _, proc := range b.processors {
		proc.StartAsync(b.ctx, &b.wg)
	}

	b.wg.Wait()
}

////////////////////////////////////////////////////////////////////////////////
///// INTERNAL /////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// waitForSignal ожидает сигнал и вызывает cancelFunc.
func (b *Builder) waitForSignal(sig chan os.Signal, cancelFunc func()) {
	gotSig := <-sig
	log.Info().Str("sig", gotSig.String()).Msg("Запрошено завершение")
	cancelFunc()
}

// exec выполняет callback только если:
// - нет предыдущих ошибок
// - контекст не отменён
// - все requiredArgs не nil/zero
func (b *Builder) exec(cb func(b *Builder), requiredArgs ...any) {
	if b.err != nil || b.ctx.Err() != nil {
		return
	}

	for _, requiredArg := range requiredArgs {
		rv := reflect.ValueOf(requiredArg)
		if rv.Type().Kind() == reflect.Struct || !rv.IsZero() {
			continue
		}

		b.err = fmt.Errorf("BUG: required %s, but empty", rv.Type().String())
		return
	}

	cb(b)
}
