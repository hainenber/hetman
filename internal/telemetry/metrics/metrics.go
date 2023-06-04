package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

type InternalErrorLoggerHook struct{}

func (i InternalErrorLoggerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level == zerolog.ErrorLevel {
		Meters.InternalErrorCount.Add(context.Background(), 1)
	}
}

const (
	serviceName    = "hetman"
	serviceVersion = "v0.1.0"
)

type meters struct {
	IngestedLogCount      metric.Int64Counter
	ForwardedLogCount     metric.Int64Counter
	ReceivedRestartCount  metric.Int64Counter
	InternalErrorCount    metric.Int64Counter
	InitializedComponents map[string]metric.Int64UpDownCounter
}

var (
	Meters meters
)

func InitiateMetricProvider(logger *zerolog.Logger) (func(), error) {
	ctx := context.Background()

	// Instantiate insecure push-based OTLP exporter
	// TODO: Provide client security for exporter-receiver connection
	otlpExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return func() {}, err
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(serviceVersion),
	)
	meterProvider := sdkMetric.NewMeterProvider(
		sdkMetric.WithResource(res),
		sdkMetric.WithReader(sdkMetric.NewPeriodicReader(otlpExporter)),
	)

	// Set meter provider for global OpenTelemetry imports
	otel.SetMeterProvider(meterProvider)

	// Create an instance on a meter for the given instrumentation scope
	meter := meterProvider.Meter(
		"github.com/hainenber/hetman",
		metric.WithInstrumentationVersion("v0.0.0"),
	)

	// Instantiate metric submitters
	ingestedLogCount, err := meter.Int64Counter("ingestedLogCount", []metric.Int64CounterOption{
		metric.WithUnit("line"),
		metric.WithDescription("Count of ingested log"),
	}...)
	if err != nil {
		return func() {}, err
	}
	forwardedLogCount, err := meter.Int64Counter("forwardedLogCount", []metric.Int64CounterOption{
		metric.WithUnit("line"),
		metric.WithDescription("Count of forwarded log"),
	}...)
	if err != nil {
		return func() {}, err
	}
	receivedRestartCount, err := meter.Int64Counter("receivedRestartCount", []metric.Int64CounterOption{
		metric.WithDescription("Count of received restart signal"),
	}...)
	if err != nil {
		return func() {}, err
	}
	internalErrorCount, err := meter.Int64Counter("internalErrorCount", []metric.Int64CounterOption{
		metric.WithDescription("Count of internal error"),
	}...)
	if err != nil {
		return func() {}, err
	}
	initializedComponents := make(map[string]metric.Int64UpDownCounter, 4)
	for _, name := range []string{"inputs", "tailers", "buffers", "forwarders"} {
		metricName := fmt.Sprintf("initialized%s", toTitle(name))
		initializedComponent, err := meter.Int64UpDownCounter(metricName,
			metric.WithDescription("Count of initialized "+name),
		)
		if err != nil {
			return func() {}, err
		}
		initializedComponents[name] = initializedComponent
	}

	// Assign created submitters to global meter struct for wide usage
	Meters.IngestedLogCount = ingestedLogCount
	Meters.ForwardedLogCount = forwardedLogCount
	Meters.ReceivedRestartCount = receivedRestartCount
	Meters.InternalErrorCount = internalErrorCount
	Meters.InitializedComponents = initializedComponents

	return func() {
		// Shutdown meter provider
		if err := meterProvider.Shutdown(ctx); err != nil {
			logger.Error().Err(err).Msg("")
		}
	}, nil
}

func toTitle(s string) string {
	if len(s) == 0 {
		return s
	}

	return strings.ToUpper(string(s[0])) + s[1:]
}

func InitializeNopMetricProvider() (func(), error) {
	nopLogger := zerolog.Nop()

	// Mock a OTLP receiver
	otlpMockReceiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	os.Setenv("OTEL_SERVICE_NAME", "hetman")
	os.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", otlpMockReceiver.URL)

	return InitiateMetricProvider(&nopLogger)
}
