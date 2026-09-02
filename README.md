# MoM Boilerplate V2

Шаблон для создания Go микросервисов: HTTP-сервер со служебными маршрутами, конфиг, graceful shutdown, CLI, переиспользуемая библиотека Kafka (`broker`) и базовый observability-стек (OpenTelemetry-трейсинг, Sentry, Prometheus-метрики, pprof). Слои данных (Redis/PostgreSQL) и доменные обработчики/события добавляются под конкретный сервис.

## Технологии

| Компонент | Технология |
|-----------|------------|
| HTTP Server | `net/http` + `gorilla/mux` |
| Логирование | `zerolog` (+ trace_id hook) |
| Конфигурация | `envconfig` + `godotenv` |
| CLI | `urfave/cli/v2` |
| Валидация | `go-playground/validator` |
| Брокер (Kafka) | `IBM/sarama` (generic `broker.Bus[T]`) |
| Трейсинг | OpenTelemetry (OTLP/gRPC) + `otelmux` |
| Метрики | Prometheus (`/metrics`) |
| Ошибки | Sentry (`getsentry/sentry-go`) |
| Профилирование | `net/http/pprof` (`/debug/pprof`) |
| Линтер | `golangci-lint` v2 |

## Требования

| Зависимость | Версия |
|-------------|--------|
| [Go](https://go.dev) | 1.25+ |
| [Docker](https://docker.com) | 20+ |
| make | - |

## Быстрый старт

### 1. Клонирование шаблона

```bash
git clone --depth=1 git@github.com:MoM-Repo/worker-service.git YOUR_PROJECT_NAME
cd YOUR_PROJECT_NAME
```

### 2. Настройка проекта

```bash
chmod +x setup.sh
./setup.sh your-project-name your-github-username
```

Скрипт автоматически:
- Обновит все импорты в Go файлах
- Изменит `go.mod` на новый модуль
- Обновит конфигурационные файлы

### 3. Инициализация репозитория

```bash
rm -rf .git
git init
git add .
git commit -m "Initial commit"
git remote add origin git@github.com:your-username/your-project-name.git
git push -u origin main
```

### 4. Запуск

```bash
make run
```

Сервер доступен на `http://localhost:8080`

## Доступные команды

```bash
make help  # Показать все команды
```

### Разработка
| Команда | Описание |
|---------|----------|
| `make run` | Запустить приложение |
| `make build` | Собрать бинарник |
| `make test` | Запустить тесты |

### Качество кода
| Команда | Описание |
|---------|----------|
| `make lint` | Запустить линтер |
| `make lint-fix` | Линтер + автофикс |

### Окружение
| Команда | Описание |
|---------|----------|
| `make up` | Поднять локальное docker окружение |
| `make down` | Остановить контейнеры |
| `make logs` | Показать логи |

### CI/CD
| Команда | Описание |
|---------|----------|
| `make ci` | Все CI проверки |
| `make ci-full` | CI + Docker build |
| `make docker-build` | Собрать Docker образ |

## Структура проекта

```
.
├── .github/workflows/      # GitHub Actions CI
├── cmd/                    # CLI команды
├── internal/
│   ├── app/
│   │   ├── builder/        # Dependency injection
│   │   ├── config/         # Конфигурация
│   │   ├── entity/         # Модели/DTO/ошибки
│   │   ├── handler/        # HTTP обработчики
│   │   ├── monitor/        # Observability: sentry, tracelog (trace_id hook)
│   │   ├── processor/      # Процессоры (HTTP server, OpenTelemetry, graceful shutdown)
│   │   ├── service/        # Бизнес-логика
│   │   └── util/           # Утилиты
│   └── pkg/
│       ├── broker/         # Kafka-библиотека (Bus[T], codec, mock)
│       ├── constant/       # Константы
│       └── http/           # HTTP утилиты
│           ├── binding/    # Парсинг и валидация запросов
│           ├── httph/      # HTTP хелперы (error-context и пр.)
│           └── mzerolog/   # Логирование middleware
├── docker-compose.local.yml # Локальное окружение (Kafka, Jaeger, kafka-ui)
├── Dockerfile
├── Makefile
└── setup.sh                # Скрипт настройки
```

## Конфигурация

Переменные окружения (см. `.env.dist`):

```env
# Web Server
APP_PROCESSOR_WEB_SERVER_LISTEN_PORT=8080

# Monitor
APP_MONITOR_LOG_LEVEL=debug
APP_MONITOR_ENVIRONMENT=development

# Monitor: Sentry / OpenTelemetry (выключены по умолчанию)
APP_MONITOR_SENTRY_ENABLED=false
APP_MONITOR_SENTRY_DSN=
APP_MONITOR_OPEN_TELEMETRY_ENABLED=false
APP_MONITOR_OPEN_TELEMETRY_ADDRESS=localhost:4317
APP_MONITOR_OPEN_TELEMETRY_MAX_QUEUE_SIZE=2048
APP_MONITOR_OPEN_TELEMETRY_MAX_BATCH_SIZE=512
APP_MONITOR_OPEN_TELEMETRY_SEND_BATCH_TIMEOUT=5s
APP_MONITOR_OPEN_TELEMETRY_EXPORT_TIMEOUT=30s
APP_MONITOR_OPEN_TELEMETRY_SAMPLE_RATIO=1

# Broker (Kafka) — укажите ADDRESSES, чтобы включить
APP_BROKER_KAFKA_ADDRESSES=
APP_BROKER_KAFKA_CONSUMER_GROUP=
APP_BROKER_KAFKA_CLIENT_ID=
```

## API Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus-метрики |
| GET | `/debug/pprof` | Профилировщик (pprof) |

```bash
curl http://localhost:8080/health
```

## CI/CD

GitHub Actions выполняет:
- Build
- Test (с coverage)
- Lint (golangci-lint)
- Mod check (go.mod/go.sum)
- Docker build
