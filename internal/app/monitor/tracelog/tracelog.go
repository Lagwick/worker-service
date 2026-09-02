package mtracelog

import (
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

const fieldTraceID = "trace_id"

type Hook struct{}

func (Hook) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	sc := trace.SpanContextFromContext(e.GetCtx())
	if !sc.IsValid() {
		return
	}

	e.Str(fieldTraceID, sc.TraceID().String())
}
