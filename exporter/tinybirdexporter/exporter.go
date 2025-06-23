// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tinybirdexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter/internal"
)

const (
	headerRetryAfter  = "Retry-After"
	contentTypeNDJSON = "application/x-ndjson"
)

type tinybirdExporter struct {
	config    *Config
	client    *http.Client
	logger    *zap.Logger
	settings  component.TelemetrySettings
	userAgent string
}

func newExporter(cfg component.Config, set exporter.Settings) (*tinybirdExporter, error) {
	oCfg := cfg.(*Config)

	userAgent := fmt.Sprintf("%s/%s (%s/%s)",
		set.BuildInfo.Description, set.BuildInfo.Version, runtime.GOOS, runtime.GOARCH)

	return &tinybirdExporter{
		config:    oCfg,
		logger:    set.Logger,
		userAgent: userAgent,
		settings:  set.TelemetrySettings,
	}, nil
}

func (e *tinybirdExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.config.ClientConfig.ToClient(ctx, host, e.settings)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

func (e *tinybirdExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	buffer := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(buffer)
	err := internal.ConvertTraces(td, encoder)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return e.export(ctx, e.config.Traces.Datasource, buffer)
}

func (e *tinybirdExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	sumBuffer := bytes.NewBuffer(nil)
	sumEncoder := json.NewEncoder(sumBuffer)

	gaugeBuffer := bytes.NewBuffer(nil)
	gaugeEncoder := json.NewEncoder(gaugeBuffer)

	histogramBuffer := bytes.NewBuffer(nil)
	histogramEncoder := json.NewEncoder(histogramBuffer)

	exponentialHistogramBuffer := bytes.NewBuffer(nil)
	exponentialHistogramEncoder := json.NewEncoder(exponentialHistogramBuffer)

	err := internal.ConvertMetrics(md, sumEncoder, gaugeEncoder, histogramEncoder, exponentialHistogramEncoder)
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	if sumBuffer.Len() > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsSum.Datasource, sumBuffer))
	}
	if gaugeBuffer.Len() > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsGauge.Datasource, gaugeBuffer))
	}
	if histogramBuffer.Len() > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsHistogram.Datasource, histogramBuffer))
	}
	if exponentialHistogramBuffer.Len() > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsExponentialHistogram.Datasource, exponentialHistogramBuffer))
	}
	return err
}

func (e *tinybirdExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	buffer := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(buffer)
	err := internal.ConvertLogs(ld, encoder)
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.config.Logs.Datasource, buffer)
}

func (e *tinybirdExporter) export(ctx context.Context, dataSource string, buffer *bytes.Buffer) error {
	// Create request and add query parameters
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.ClientConfig.Endpoint+"/v0/events", buffer)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	q := req.URL.Query()
	q.Set("name", dataSource)
	if e.config.Wait {
		q.Set("wait", "true")
	}
	req.URL.RawQuery = q.Encode()

	// Set headers
	req.Header.Set("Content-Type", contentTypeNDJSON)
	req.Header.Set("Authorization", "Bearer "+string(e.config.Token))
	req.Header.Set("User-Agent", e.userAgent)

	// Send request
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Handle response
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Read error response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Check if retryable
	isThrottleError := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable
	if isThrottleError {
		formattedErr := fmt.Errorf("request throttled")

		// Use Values to check if the header is present, and if present even if it is empty return ThrottleRetry.
		values := resp.Header.Values(headerRetryAfter)
		if len(values) == 0 {
			return formattedErr
		}
		// The value of Retry-After field can be either an HTTP-date or a number of
		// seconds to delay after the response is received. See https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3
		//
		// Retry-After = HTTP-date / delay-seconds
		//
		// First try to parse delay-seconds, since that is what the receiver will send.
		if seconds, err := strconv.Atoi(values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Duration(seconds)*time.Second)
		}
		if date, err := time.Parse(time.RFC1123, values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Until(date))
		}
	}

	return consumererror.NewPermanent(fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body)))
}
