// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tinybirdexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter"

import (
	"fmt"
	"net/url"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for the Tinybird exporter.
type Config struct {
	ClientConfig confighttp.ClientConfig         `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	RetryConfig  configretry.BackOffConfig       `mapstructure:"retry_on_failure"`
	QueueConfig  exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`

	// Tinybird API token.
	Token string `mapstructure:"token"`
	// Metric datasource name.
	MetricsDataSource string `mapstructure:"metrics_datasource"`
	// Traces datasource name.
	TracesDataSource string `mapstructure:"traces_datasource"`
	// Logs datasource name.
	LogsDatasource string `mapstructure:"logs_datasource"`
	// Wait for data to be ingested before returning a response.
	Wait bool `mapstructure:"wait"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if cfg.ClientConfig.Endpoint == "" {
		return errMissingEndpoint
	}
	u, err := url.Parse(cfg.ClientConfig.Endpoint)
	if err != nil {
		return fmt.Errorf("endpoint must be a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint must have http or https scheme: %s", cfg.ClientConfig.Endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("endpoint must have a host: %s", cfg.ClientConfig.Endpoint)
	}
	if cfg.Token == "" {
		return errMissingToken
	}
	if cfg.MetricsDataSource == "" {
		return fmt.Errorf("metrics_datasource must be configured")
	}
	if cfg.TracesDataSource == "" {
		return fmt.Errorf("traces_datasource must be configured")
	}
	if cfg.LogsDatasource == "" {
		return fmt.Errorf("logs_datasource must be configured")
	}
	return nil
}
