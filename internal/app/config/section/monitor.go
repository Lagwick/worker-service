package section

import "time"

type (
	// Monitor — конфигурация наблюдаемости и логирования.
	Monitor struct {
		LogLevel      string `default:"debug" split_words:"true"`
		Environment   string `default:"dev"`
		Sentry        MonitorSentry
		OpenTelemetry MonitorOpenTelemetry `split_words:"true"`
	}

	MonitorSentry struct {
		Enabled bool   `default:"false"`
		DSN     string `default:""`
	}

	MonitorOpenTelemetry struct {
		Enabled          bool          `default:"false"`
		Address          string        `default:""`
		MaxQueueSize     uint64        `default:"2048" split_words:"true"`
		MaxBatchSize     uint64        `default:"512" split_words:"true"`
		SendBatchTimeout time.Duration `default:"5s" split_words:"true"`
		ExportTimeout    time.Duration `default:"30s" split_words:"true"`
		SampleRatio      float64       `default:"1" split_words:"true"`
	}
)
