package tinybirdexporter

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter/internal/metadata"
)

func TestType(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, metadata.Type, f.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, "https://api.tinybird.co/v0/events", cfg.Endpoint)
	assert.Equal(t, exporterhelper.NewDefaultQueueConfig(), cfg.QueueSettings)
	assert.Equal(t, configretry.NewDefaultBackOffConfig(), cfg.BackOffConfig)
	expectedClient := confighttp.NewDefaultClientConfig()
	expectedClient.Timeout = 5 * time.Second
	assert.Equal(t, expectedClient, cfg.ClientConfig)
}
