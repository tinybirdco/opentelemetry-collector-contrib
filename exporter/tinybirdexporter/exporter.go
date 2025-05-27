package tinybirdexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// tinybirdExporter sends logs to Tinybird Events API.
type tinybirdExporter struct {
	cfg      *Config
	settings exporter.Settings
	client   *http.Client
	endpoint string
	logger   *zap.Logger
}

func newTinybirdExporter(cfg *Config, set exporter.Settings) *tinybirdExporter {
	return &tinybirdExporter{
		cfg:      cfg,
		settings: set,
		logger:   set.Logger,
	}
}

func (t *tinybirdExporter) start(ctx context.Context, host component.Host) error {
	client, err := t.cfg.ToClient(ctx, host, t.settings.TelemetrySettings)
	if err != nil {
		return err
	}
	t.client = client
	t.endpoint = strings.TrimRight(t.cfg.Endpoint, "/") + "?name=" + url.QueryEscape(t.cfg.Datasource)
	return nil
}

func (t *tinybirdExporter) shutdown(context.Context) error {
	return nil
}

func attributesToMap(attrs plog.Map) map[string]any {
	m := make(map[string]any, attrs.Len())
	attrs.Range(func(k string, v plog.Value) bool {
		m[k] = v.AsRaw()
		return true
	})
	return m
}

func (t *tinybirdExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	var buf bytes.Buffer
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resAttrs := attributesToMap(rl.Resource().Attributes())
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			logs := sl.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				event := map[string]any{
					"timestamp":       lr.Timestamp().AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"),
					"severity_number": int(lr.SeverityNumber()),
					"severity_text":   lr.SeverityText(),
					"body":            lr.Body().AsRaw(),
					"trace_id":        lr.TraceID().HexString(),
					"span_id":         lr.SpanID().HexString(),
				}
				for k, v := range resAttrs {
					event[k] = v
				}
				lr.Attributes().Range(func(k string, v plog.Value) bool {
					event[k] = v.AsRaw()
					return true
				})
				b, err := json.Marshal(event)
				if err != nil {
					return consumererror.NewPermanent(err)
				}
				buf.Write(b)
				buf.WriteByte('\n')
			}
		}
	}
	if buf.Len() == 0 {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if t.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+string(t.cfg.Token))
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	err = fmt.Errorf("tinybird error %d: %s", resp.StatusCode, string(body))

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		retryAfter := 0
		if val := resp.Header.Get("Retry-After"); val != "" {
			if seconds, err2 := strconv.Atoi(val); err2 == nil {
				retryAfter = seconds
			}
		}
		if retryAfter == 0 {
			if val := resp.Header.Get("X-RateLimit-Reset"); val != "" {
				if ts, err2 := strconv.ParseInt(val, 10, 64); err2 == nil {
					wait := time.Until(time.Unix(ts, 0))
					if wait > 0 {
						retryAfter = int(wait.Seconds())
					}
				}
			}
		}
		return exporterhelper.NewThrottleRetry(err, time.Duration(retryAfter)*time.Second)
	}
	if resp.StatusCode >= 500 {
		return err
	}
	return consumererror.NewPermanent(err)
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp := newTinybirdExporter(c, set)
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.pushLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
	)
}
