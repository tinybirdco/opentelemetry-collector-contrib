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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

const (
	headerRetryAfter  = "Retry-After"
	contentTypeNDJSON = "application/x-ndjson"
)

func convertAttributes(attributes pcommon.Map) map[string]string {
	attrs := make(map[string]string, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		attrs[k] = v.AsString()
		return true
	})
	return attrs
}

// Event represents any type of event that can be exported
type Event interface {
	// ensure only our event types can implement this interface
	event()
}

type baseEvent struct {
	ResourceSchemaUrl  string            `json:"resource_schema_url"`
	ResourceAttributes map[string]string `json:"resource_attributes"`
	ServiceName        string            `json:"service_name"`
	ScopeName          string            `json:"scope_name"`
	ScopeVersion       string            `json:"scope_version"`
	ScopeSchemaUrl     string            `json:"scope_schema_url"`
	ScopeAttributes    map[string]string `json:"scope_attributes"`
}

func newBaseEvent(resource pcommon.Resource, scope pcommon.InstrumentationScope, resourceSchemaUrl string, scopeSchemaUrl string) baseEvent {
	serviceName := ""
	if v, ok := resource.Attributes().Get(string(conventions.ServiceNameKey)); ok {
		serviceName = v.Str()
	}
	return baseEvent{
		ResourceSchemaUrl:  resourceSchemaUrl,
		ResourceAttributes: convertAttributes(resource.Attributes()),
		ServiceName:        serviceName,
		ScopeSchemaUrl:     scopeSchemaUrl,
		ScopeName:          scope.Name(),
		ScopeVersion:       scope.Version(),
		ScopeAttributes:    convertAttributes(scope.Attributes()),
	}
}

type traceEvent struct {
	baseEvent
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	ParentSpanID   string            `json:"parent_span_id"`
	TraceState     string            `json:"trace_state"`
	TraceFlags     uint32            `json:"trace_flags"`
	SpanName       string            `json:"span_name"`
	SpanKind       string            `json:"span_kind"`
	SpanAttributes map[string]string `json:"span_attributes"`
	StartTime      string            `json:"start_time"`
	// Format start-end
	EndTime string `json:"end_time,omitempty"`
	// format start-duration
	Duration         int64               `json:"duration,omitempty"`
	StatusCode       string              `json:"status_code"`
	StatusMessage    string              `json:"status_message"`
	EventsTimestamp  []string            `json:"events_timestamp"`
	EventsName       []string            `json:"events_name"`
	EventsAttributes []map[string]string `json:"events_attributes"`
	LinksTraceID     []string            `json:"links_trace_id"`
	LinksSpanID      []string            `json:"links_span_id"`
	LinksTraceState  []string            `json:"links_trace_state"`
	LinksAttributes  []map[string]string `json:"links_attributes"`
}

func (traceEvent) event() {}

type sumMetric struct {
	metricEvent
	ExemplarsTraceId            []string            `json:"exemplars_trace_id"`
	ExemplarsSpanId             []string            `json:"exemplars_span_id"`
	ExemplarsTimestamp          []string            `json:"exemplars_timestamp"`
	ExemplarsFilteredAttributes []map[string]string `json:"exemplars_filtered_attributes"`
	ExemplarsValue              []float64           `json:"exemplars_value"`
	Value                       float64             `json:"value"`
	AggregationTemporality      int32               `json:"aggregation_temporality"`
	IsMonotonic                 bool                `json:"is_monotonic"`
}

func (sumMetric) event() {}

type histogramMetric struct {
	metricEvent
	ExemplarsTraceId            []string            `json:"exemplars_trace_id"`
	ExemplarsSpanId             []string            `json:"exemplars_span_id"`
	ExemplarsTimestamp          []string            `json:"exemplars_timestamp"`
	ExemplarsFilteredAttributes []map[string]string `json:"exemplars_filtered_attributes"`
	ExemplarsValue              []float64           `json:"exemplars_value"`
	Count                       uint64              `json:"count"`
	Sum                         float64             `json:"sum"`
	BucketCounts                []uint64            `json:"bucket_counts"`
	ExplicitBounds              []float64           `json:"explicit_bounds"`
	Min                         *float64            `json:"min,omitempty"`
	Max                         *float64            `json:"max,omitempty"`
	AggregationTemporality      int32               `json:"aggregation_temporality"`
}

func (histogramMetric) event() {}

type exponentialHistogramMetrics struct {
	metricEvent
	Count                       uint64              `json:"count"`
	Sum                         float64             `json:"sum"`
	Scale                       int32               `json:"scale"`
	ZeroCount                   uint64              `json:"zero_count"`
	PositiveOffset              int32               `json:"positive_offset"`
	PositiveBucketCounts        []uint64            `json:"positive_bucket_counts"`
	NegativeOffset              int32               `json:"negative_offset"`
	NegativeBucketCounts        []uint64            `json:"negative_bucket_counts"`
	Min                         *float64            `json:"min,omitempty"`
	Max                         *float64            `json:"max,omitempty"`
	AggregationTemporality      int32               `json:"aggregation_temporality"`
	ExemplarsFilteredAttributes []map[string]string `json:"exemplars_filtered_attributes"`
	ExemplarsTimestamp          []string            `json:"exemplars_timestamp"`
	ExemplarsValue              []float64           `json:"exemplars_value"`
	ExemplarsSpanId             []string            `json:"exemplars_span_id"`
	ExemplarsTraceId            []string            `json:"exemplars_trace_id"`
}

func (exponentialHistogramMetrics) event() {}

type gaugeMetric struct {
	metricEvent
	ExemplarsTraceId            []string            `json:"exemplars_trace_id"`
	ExemplarsSpanId             []string            `json:"exemplars_span_id"`
	ExemplarsTimestamp          []string            `json:"exemplars_timestamp"`
	ExemplarsFilteredAttributes []map[string]string `json:"exemplars_filtered_attributes"`
	ExemplarsValue              []float64           `json:"exemplars_value"`
	Value                       float64             `json:"value"`
}

func (gaugeMetric) event() {}

type metricEvent struct {
	baseEvent
	MetricName        string            `json:"metric_name"`
	MetricDescription string            `json:"metric_description"`
	MetricUnit        string            `json:"metric_unit"`
	MetricAttributes  map[string]string `json:"metric_attributes"`
	StartTimestamp    string            `json:"start_timestamp"`
	Timestamp         string            `json:"timestamp"`
	Flags             uint32            `json:"flags"`
}

func (metricEvent) event() {}

type logEvent struct {
	baseEvent
	Timestamp      string            `json:"timestamp"`
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	Flags          uint32            `json:"flags"`
	SeverityText   string            `json:"severity_text"`
	SeverityNumber int32             `json:"severity_number"`
	LogAttributes  map[string]string `json:"log_attributes"`
	Body           string            `json:"body"`
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
	client, err := e.config.ClientConfig.ToClient(ctx, host, e.settings)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

func convertEvents(events ptrace.SpanEventSlice) ([]string, []string, []map[string]string) {
	timestamps := make([]string, events.Len())
	names := make([]string, events.Len())
	attributes := make([]map[string]string, events.Len())
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		timestamps[i] = event.Timestamp().AsTime().Format(time.RFC3339Nano)
		names[i] = event.Name()
		attributes[i] = convertAttributes(event.Attributes())
	}
	return timestamps, names, attributes
}

func convertLinks(links ptrace.SpanLinkSlice) ([]string, []string, []string, []map[string]string) {
	traceIDs := make([]string, links.Len())
	spanIDs := make([]string, links.Len())
	states := make([]string, links.Len())
	attrs := make([]map[string]string, links.Len())
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs[i] = traceutil.TraceIDToHexOrEmptyString(link.TraceID())
		spanIDs[i] = traceutil.SpanIDToHexOrEmptyString(link.SpanID())
		states[i] = link.TraceState().AsRaw()
		attrs[i] = convertAttributes(link.Attributes())
	}
	return traceIDs, spanIDs, states, attrs
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
				eventTimes, eventNames, eventAttrs := convertEvents(span.Events())
				linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(span.Links())
				event := traceEvent{
					baseEvent:        newBaseEvent(resource, scope, schemaUrl, scopeSchemaUrl),
					TraceID:          traceutil.TraceIDToHexOrEmptyString(span.TraceID()),
					SpanID:           traceutil.SpanIDToHexOrEmptyString(span.SpanID()),
					ParentSpanID:     traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()),
					TraceState:       span.TraceState().AsRaw(),
					TraceFlags:       span.Flags(),
					SpanName:         span.Name(),
					SpanKind:         span.Kind().String(),
					SpanAttributes:   convertAttributes(attributes),
					StartTime:        span.StartTimestamp().AsTime().Format(time.RFC3339Nano),
					EndTime:          span.EndTimestamp().AsTime().Format(time.RFC3339Nano),
					Duration:         span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds(),
					StatusCode:       span.Status().Code().String(),
					StatusMessage:    span.Status().Message(),
					EventsTimestamp:  eventTimes,
					EventsName:       eventNames,
					EventsAttributes: eventAttrs,
					LinksTraceID:     linksTraceIDs,
					LinksSpanID:      linksSpanIDs,
					LinksTraceState:  linksTraceStates,
					LinksAttributes:  linksAttrs,
				}
				events = append(events, event)
			}
		}
	}

	return e.export(ctx, e.config.Traces.Datasource, events)
}

func (e *tinybirdExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	metricCount := md.MetricCount()
	gaugeEvents := make([]Event, 0, metricCount)
	sumEvents := make([]Event, 0, metricCount)
	histogramEvents := make([]Event, 0, metricCount)
	exponentialHistogramEvents := make([]Event, 0, metricCount)

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
				base := func() metricEvent {
					return metricEvent{
						baseEvent:         newBaseEvent(resource, scope, schemaUrl, scopeSchemaUrl),
						MetricName:        metric.Name(),
						MetricDescription: metric.Description(),
						MetricUnit:        metric.Unit(),
					}
				}()

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						me := base
						me.MetricAttributes = convertAttributes(dp.Attributes())
						me.StartTimestamp = dp.StartTimestamp().AsTime().Format(time.RFC3339Nano)
						me.Timestamp = dp.Timestamp().AsTime().Format(time.RFC3339Nano)
						me.Flags = uint32(dp.Flags())
						var value float64
						switch dp.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							value = float64(dp.IntValue())
						case pmetric.NumberDataPointValueTypeDouble:
							value = dp.DoubleValue()
						case pmetric.NumberDataPointValueTypeEmpty:
							value = 0.0
						}
						filteredAttrs, timestamps, values, spanIDs, traceIDs := computeExemplars(dp.Exemplars())
						gaugeEvents = append(gaugeEvents, gaugeMetric{
							metricEvent:                 me,
							Value:                       value,
							ExemplarsFilteredAttributes: filteredAttrs,
							ExemplarsTimestamp:          timestamps,
							ExemplarsValue:              values,
							ExemplarsSpanId:             spanIDs,
							ExemplarsTraceId:            traceIDs,
						})
					}
				case pmetric.MetricTypeSum:
					sum := metric.Sum()
					dps := sum.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						me := base
						me.MetricAttributes = convertAttributes(dp.Attributes())
						me.StartTimestamp = dp.StartTimestamp().AsTime().Format(time.RFC3339Nano)
						me.Timestamp = dp.Timestamp().AsTime().Format(time.RFC3339Nano)
						me.Flags = uint32(dp.Flags())
						var value float64
						switch dp.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							value = float64(dp.IntValue())
						case pmetric.NumberDataPointValueTypeDouble:
							value = dp.DoubleValue()
						case pmetric.NumberDataPointValueTypeEmpty:
							value = 0.0
						}
						filteredAttrs, timestamps, values, spanIDs, traceIDs := computeExemplars(dp.Exemplars())
						sumEvents = append(sumEvents, sumMetric{
							metricEvent:                 me,
							Value:                       value,
							AggregationTemporality:      int32(sum.AggregationTemporality()),
							IsMonotonic:                 sum.IsMonotonic(),
							ExemplarsFilteredAttributes: filteredAttrs,
							ExemplarsTimestamp:          timestamps,
							ExemplarsValue:              values,
							ExemplarsSpanId:             spanIDs,
							ExemplarsTraceId:            traceIDs,
						})
					}
				case pmetric.MetricTypeHistogram:
					hist := metric.Histogram()
					dps := hist.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						me := base
						me.MetricAttributes = convertAttributes(dp.Attributes())
						me.StartTimestamp = dp.StartTimestamp().AsTime().Format(time.RFC3339Nano)
						me.Timestamp = dp.Timestamp().AsTime().Format(time.RFC3339Nano)
						me.Flags = uint32(dp.Flags())
						var minVal, maxVal *float64
						if dp.HasMin() {
							localMin := dp.Min()
							minVal = &localMin
						}
						if dp.HasMax() {
							localMax := dp.Max()
							maxVal = &localMax
						}
						filteredAttrs, timestamps, values, spanIDs, traceIDs := computeExemplars(dp.Exemplars())
						histogramEvents = append(histogramEvents, histogramMetric{
							metricEvent:                 me,
							Count:                       dp.Count(),
							Sum:                         dp.Sum(),
							BucketCounts:                dp.BucketCounts().AsRaw(),
							ExplicitBounds:              dp.ExplicitBounds().AsRaw(),
							Min:                         minVal,
							Max:                         maxVal,
							AggregationTemporality:      int32(hist.AggregationTemporality()),
							ExemplarsFilteredAttributes: filteredAttrs,
							ExemplarsTimestamp:          timestamps,
							ExemplarsValue:              values,
							ExemplarsSpanId:             spanIDs,
							ExemplarsTraceId:            traceIDs,
						})
					}
				case pmetric.MetricTypeExponentialHistogram:
					ehist := metric.ExponentialHistogram()
					dps := ehist.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						me := base
						me.MetricAttributes = convertAttributes(dp.Attributes())
						me.StartTimestamp = dp.StartTimestamp().AsTime().Format(time.RFC3339Nano)
						me.Timestamp = dp.Timestamp().AsTime().Format(time.RFC3339Nano)
						me.Flags = uint32(dp.Flags())
						var minVal, maxVal *float64
						if dp.HasMin() {
							localMin := dp.Min()
							minVal = &localMin
						}
						if dp.HasMax() {
							localMax := dp.Max()
							maxVal = &localMax
						}
						filteredAttrs, timestamps, values, spanIDs, traceIDs := computeExemplars(dp.Exemplars())
						exponentialHistogramEvents = append(exponentialHistogramEvents, exponentialHistogramMetrics{
							metricEvent:                 me,
							Count:                       dp.Count(),
							Sum:                         dp.Sum(),
							Scale:                       dp.Scale(),
							ZeroCount:                   dp.ZeroCount(),
							PositiveOffset:              dp.Positive().Offset(),
							PositiveBucketCounts:        dp.Positive().BucketCounts().AsRaw(),
							NegativeOffset:              dp.Negative().Offset(),
							NegativeBucketCounts:        dp.Negative().BucketCounts().AsRaw(),
							Min:                         minVal,
							Max:                         maxVal,
							AggregationTemporality:      int32(ehist.AggregationTemporality()),
							ExemplarsFilteredAttributes: filteredAttrs,
							ExemplarsTimestamp:          timestamps,
							ExemplarsValue:              values,
							ExemplarsSpanId:             spanIDs,
							ExemplarsTraceId:            traceIDs,
						})
					}
				}
			}
		}
	}

	var err error
	if len(gaugeEvents) > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsGauge.Datasource, gaugeEvents))
	}
	if len(sumEvents) > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsSum.Datasource, sumEvents))
	}
	if len(histogramEvents) > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsHistogram.Datasource, histogramEvents))
	}
	if len(exponentialHistogramEvents) > 0 {
		err = errors.Join(err, e.export(ctx, e.config.MetricsExponentialHistogram.Datasource, exponentialHistogramEvents))
	}
	return err
}

// Helper to fill exemplars into a metricEvent
func computeExemplars(exemplars pmetric.ExemplarSlice) ([]map[string]string, []string, []float64, []string, []string) {
	filteredAttributes := make([]map[string]string, exemplars.Len())
	timestamps := make([]string, exemplars.Len())
	values := make([]float64, exemplars.Len())
	spanIDs := make([]string, exemplars.Len())
	traceIDs := make([]string, exemplars.Len())
	for i := 0; i < exemplars.Len(); i++ {
		ex := exemplars.At(i)
		filteredAttributes[i] = convertAttributes(ex.FilteredAttributes())
		timestamps[i] = ex.Timestamp().AsTime().Format(time.RFC3339Nano)
		var value float64
		switch ex.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			value = float64(ex.IntValue())
		case pmetric.ExemplarValueTypeDouble:
			value = ex.DoubleValue()
		case pmetric.ExemplarValueTypeEmpty:
			// Value is unset, use 0.0 as default
			value = 0.0
		}
		values[i] = value
		spanIDs[i] = traceutil.SpanIDToHexOrEmptyString(ex.SpanID())
		traceIDs[i] = traceutil.TraceIDToHexOrEmptyString(ex.TraceID())
	}
	return filteredAttributes, timestamps, values, spanIDs, traceIDs
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
				event := logEvent{
					baseEvent:      newBaseEvent(resource, scope, schemaUrl, scopeSchemaUrl),
					Timestamp:      log.Timestamp().AsTime().Format(time.RFC3339Nano),
					SeverityText:   log.SeverityText(),
					SeverityNumber: int32(log.SeverityNumber()),
					LogAttributes:  convertAttributes(log.Attributes()),
					Body:           log.Body().AsString(),
					TraceID:        traceutil.TraceIDToHexOrEmptyString(log.TraceID()),
					SpanID:         traceutil.SpanIDToHexOrEmptyString(log.SpanID()),
					Flags:          uint32(log.Flags()),
				}
				events = append(events, event)
			}
		}
	}

	return e.export(ctx, e.config.Logs.Datasource, events)
}

func (e *tinybirdExporter) export(ctx context.Context, dataSource string, events []Event) error {
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
