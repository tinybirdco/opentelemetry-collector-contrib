// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tinybirdexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter"

import (
	"fmt"
	"net/url"
	"regexp"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var datasourceRegex = regexp.MustCompile(`^[\w_]+$`)

type SignalConfig struct {
	Datasource string `mapstructure:"datasource"`

	_ struct{}
}

func (cfg SignalConfig) Validate() error {
	if !datasourceRegex.MatchString(cfg.Datasource) {
		return fmt.Errorf("invalid datasource %q: only letters, numbers, and underscores are allowed", cfg.Datasource)
	}
	return nil
}

// Config defines configuration for the Tinybird exporter.
type Config struct {
	RetryConfig configretry.BackOffConfig       `mapstructure:"retry_on_failure"`
	QueueConfig exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`

	Endpoint string              `mapstructure:"endpoint"`
	Token    configopaque.String `mapstructure:"token"`
	Metrics  SignalConfig        `mapstructure:"metrics"`
	Traces   SignalConfig        `mapstructure:"traces"`
	Logs     SignalConfig        `mapstructure:"logs"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errMissingEndpoint
	}
	if cfg.Token == "" {
		return errMissingToken
	}
	if cfg.Endpoint == "" {
		return errMissingEndpoint
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("endpoint must be a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint must have http or https scheme: %q", cfg.Endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("endpoint must have a host: %q", cfg.Endpoint)
	}
	return nil
}
