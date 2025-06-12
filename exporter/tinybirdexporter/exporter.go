// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tinybirdexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter"

import (
	"bytes"
	"context"
	"encoding/json"
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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.27.0"
)

const (
	headerRetryAfter  = "Retry-After"
	contentTypeNDJSON = "application/x-ndjson"
)

// Event represents any type of event that can be exported
type Event interface {
	// ensure only our event types can implement this interface
	event()
}

type baseEvent struct {
	Type               string         `json:"type"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	ScopeName          string         `json:"scope_name"`
	ScopeVersion       string         `json:"scope_version"`
	ScopeAttributes    map[string]any `json:"scope_attributes"`
	ResourceSchemaUrl  string         `json:"resource_schema_url,omitempty"`
	ScopeSchemaUrl     string         `json:"scope_schema_url,omitempty"`
	ServiceName        string         `json:"service_name,omitempty"`
	Attributes         map[string]any `json:"attributes"`
}

func newBaseEvent(dataType string, resource pcommon.Resource, scope pcommon.InstrumentationScope, attributes pcommon.Map, resourceSchemaUrl string, scopeSchemaUrl string) baseEvent {
	serviceName := ""
	if v, ok := resource.Attributes().Get(string(conventions.ServiceNameKey)); ok {
		serviceName = v.Str()
	}
	return baseEvent{
		Type:               dataType,
		ResourceAttributes: resource.Attributes().AsRaw(),
		ScopeName:          scope.Name(),
		ScopeVersion:       scope.Version(),
		ScopeAttributes:    scope.Attributes().AsRaw(),
		ResourceSchemaUrl:  resourceSchemaUrl,
		ScopeSchemaUrl:     scopeSchemaUrl,
		ServiceName:        serviceName,
		Attributes:         attributes.AsRaw(),
	}
}

type traceEvent struct {
	baseEvent
	TraceID       string `json:"trace_id"`
	SpanID        string `json:"span_id"`
	ParentSpanID  string `json:"parent_span_id"`
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	StartTime     string `json:"start_time"`
	EndTime       string `json:"end_time"`
	StatusCode    string `json:"status_code"`
	StatusMessage string `json:"status_message"`
	Events        int    `json:"events"`
	Links         int    `json:"links"`
	TraceFlags    uint32 `json:"trace_flags,omitempty"`
}

func (traceEvent) event() {}

type metricEvent struct {
	baseEvent
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Unit        string  `json:"unit"`
	Type        string  `json:"type"`
	Value       float64 `json:"value,omitempty"`
	Count       uint64  `json:"count,omitempty"`
	Sum         float64 `json:"sum,omitempty"`
	Timestamp   string  `json:"timestamp"`
}

func (metricEvent) event() {}

type logEvent struct {
	baseEvent
	Timestamp      string `json:"timestamp"`
	SeverityText   string `json:"severity_text,omitempty"`
	SeverityNumber int    `json:"severity_number,omitempty"`
	Body           string `json:"body"`
	TraceID        string `json:"trace_id"`
	SpanID         string `json:"span_id"`
	TraceFlags     uint32 `json:"trace_flags,omitempty"`
}

func (logEvent) event() {}

type tinybirdExporter struct {
	config    *Config
	client    *http.Client
	logger    *zap.Logger
	settings  component.TelemetrySettings
	userAgent string
}

func newExporter(cfg component.Config, set exporter.Settings) (*tinybirdExporter, error) {
	oCfg := cfg.(*Config)

	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

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
	e.client = &http.Client{
		Timeout: 30 * time.Second,
	}
	return nil
}

func (e *tinybirdExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	events := make([]Event, 0, td.SpanCount())
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resource := rs.Resource()
		schemaUrl := rs.SchemaUrl()
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			scope := ss.Scope()
			scopeSchemaUrl := ss.SchemaUrl()
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				attributes := span.Attributes()
				event := traceEvent{
					baseEvent:     newBaseEvent("traces", resource, scope, attributes, schemaUrl, scopeSchemaUrl),
					TraceID:       span.TraceID().String(),
					SpanID:        span.SpanID().String(),
					ParentSpanID:  span.ParentSpanID().String(),
					Name:          span.Name(),
					Kind:          span.Kind().String(),
					StartTime:     span.StartTimestamp().AsTime().Format(time.RFC3339Nano),
					EndTime:       span.EndTimestamp().AsTime().Format(time.RFC3339Nano),
					StatusCode:    span.Status().Code().String(),
					StatusMessage: span.Status().Message(),
					Events:        span.Events().Len(),
					Links:         span.Links().Len(),
					TraceFlags:    span.Flags() & 0xff,
				}
				events = append(events, event)
			}
		}
	}

	return e.export(ctx, "traces", e.config.TracesDataSource, events)
}

func (e *tinybirdExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	events := make([]Event, 0, md.MetricCount())
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := rm.Resource()
		schemaUrl := rm.SchemaUrl()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scope := sm.Scope()
			scopeSchemaUrl := sm.SchemaUrl()
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						attributes := dp.Attributes()
						event := metricEvent{
							baseEvent:   newBaseEvent("metrics", resource, scope, attributes, schemaUrl, scopeSchemaUrl),
							Name:        metric.Name(),
							Description: metric.Description(),
							Unit:        metric.Unit(),
							Type:        metric.Type().String(),
							Value:       dp.DoubleValue(),
							Timestamp:   dp.Timestamp().AsTime().Format(time.RFC3339Nano),
						}
						events = append(events, event)
					}
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						attributes := dp.Attributes()
						event := metricEvent{
							baseEvent:   newBaseEvent("metrics", resource, scope, attributes, schemaUrl, scopeSchemaUrl),
							Name:        metric.Name(),
							Description: metric.Description(),
							Unit:        metric.Unit(),
							Type:        metric.Type().String(),
							Value:       dp.DoubleValue(),
							Timestamp:   dp.Timestamp().AsTime().Format(time.RFC3339Nano),
						}
						events = append(events, event)
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						attributes := dp.Attributes()
						event := metricEvent{
							baseEvent:   newBaseEvent("metrics", resource, scope, attributes, schemaUrl, scopeSchemaUrl),
							Name:        metric.Name(),
							Description: metric.Description(),
							Unit:        metric.Unit(),
							Type:        metric.Type().String(),
							Count:       dp.Count(),
							Sum:         dp.Sum(),
							Timestamp:   dp.Timestamp().AsTime().Format(time.RFC3339Nano),
						}
						events = append(events, event)
					}
				}
			}
		}
	}

	return e.export(ctx, "metrics", e.config.MetricsDataSource, events)
}

func (e *tinybirdExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	events := make([]Event, 0, ld.LogRecordCount())
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resource := rl.Resource()
		schemaUrl := rl.SchemaUrl()
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			scope := sl.Scope()
			scopeSchemaUrl := sl.SchemaUrl()
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				attributes := log.Attributes()
				event := logEvent{
					baseEvent:      newBaseEvent("logs", resource, scope, attributes, schemaUrl, scopeSchemaUrl),
					Timestamp:      log.Timestamp().AsTime().Format(time.RFC3339Nano),
					SeverityText:   log.SeverityText(),
					SeverityNumber: int(log.SeverityNumber()),
					Body:           log.Body().AsString(),
					TraceID:        log.TraceID().String(),
					SpanID:         log.SpanID().String(),
					TraceFlags:     uint32(log.Flags()) & 0xff,
				}
				events = append(events, event)
			}
		}
	}

	return e.export(ctx, "logs", e.config.LogsDatasource, events)
}

func (e *tinybirdExporter) export(ctx context.Context, dataType string, dataSource string, events []Event) error {
	// Convert events to NDJSON
	var buf bytes.Buffer
	for _, event := range events {
		jsonData, err := json.Marshal(event)
		if err != nil {
			return consumererror.NewPermanent(err)
		}
		buf.Write(jsonData)
		buf.WriteByte('\n')
	}

	// Create request and add query parameters
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.ClientConfig.Endpoint+"/v0/events", &buf)
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
	req.Header.Set("Authorization", "Bearer "+e.config.Token)
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
