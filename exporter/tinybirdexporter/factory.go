package tinybirdexporter

import (
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter/internal/metadata"
)

// NewFactory creates a factory for the Tinybird exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	clientCfg := confighttp.NewDefaultClientConfig()
	clientCfg.Timeout = 5 * time.Second
	return &Config{
		ClientConfig:  clientCfg,
		QueueSettings: exporterhelper.NewDefaultQueueConfig(),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		Endpoint:      "https://api.tinybird.co/v0/events",
	}
}
