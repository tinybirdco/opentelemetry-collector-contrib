package tinybirdexporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for Tinybird exporter.
type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	QueueSettings             exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// Datasource is the Tinybird datasource name to which events will be sent.
	Datasource string `mapstructure:"datasource"`

	// Token is the Tinybird API token used for authentication.
	Token configopaque.String `mapstructure:"token"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the exporter configuration.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if c.Datasource == "" {
		return errors.New("datasource is required")
	}
	if c.Token == "" {
		return errors.New("token is required")
	}
	return nil
}
