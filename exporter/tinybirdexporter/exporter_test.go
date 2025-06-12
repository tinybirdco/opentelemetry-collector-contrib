// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tinybirdexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter"

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter/internal/metadata"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestNewExporter(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				Token:             "test-token",
				MetricsDataSource: "metrics_test",
				TracesDataSource:  "traces_test",
				LogsDatasource:    "logs_test",
			},
			wantErr: false,
		},
		{
			name: "invalid endpoint",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "invalid-url",
				},
				Token:             "test-token",
				MetricsDataSource: "metrics_test",
				TracesDataSource:  "traces_test",
				LogsDatasource:    "logs_test",
			},
			wantErr: true,
		},
		{
			name: "missing token",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				MetricsDataSource: "metrics_test",
				TracesDataSource:  "traces_test",
				LogsDatasource:    "logs_test",
			},
			wantErr: true,
		},
		{
			name: "missing datasource",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				Token: "test-token",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp, err := newExporter(tt.config, exportertest.NewNopSettings(metadata.Type))
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, exp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, exp)
			}
		})
	}
}

func TestExportTraces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/v0/events", r.URL.Path)
		assert.Equal(t, "name=traces_test", r.URL.RawQuery)
		assert.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &Config{
		ClientConfig: confighttp.ClientConfig{
			Endpoint: server.URL,
		},
		Token:             "test-token",
		MetricsDataSource: "metrics_test",
		TracesDataSource:  "traces_test",
		LogsDatasource:    "logs_test",
	}

	exp, err := newExporter(config, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))

	traces := ptrace.NewTraces()
	require.NoError(t, exp.pushTraces(context.Background(), traces))
}

func TestExportMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/v0/events", r.URL.Path)
		assert.Equal(t, "name=metrics_test", r.URL.RawQuery)
		assert.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &Config{
		ClientConfig: confighttp.ClientConfig{
			Endpoint: server.URL,
		},
		Token:             "test-token",
		MetricsDataSource: "metrics_test",
		TracesDataSource:  "traces_test",
		LogsDatasource:    "logs_test",
	}

	exp, err := newExporter(config, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))

	metrics := pmetric.NewMetrics()
	require.NoError(t, exp.pushMetrics(context.Background(), metrics))
}

func TestExportLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/v0/events", r.URL.Path)
		assert.Equal(t, "name=logs_test", r.URL.RawQuery)
		assert.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &Config{
		ClientConfig: confighttp.ClientConfig{
			Endpoint: server.URL,
		},
		Token:             "test-token",
		MetricsDataSource: "metrics_test",
		TracesDataSource:  "traces_test",
		LogsDatasource:    "logs_test",
	}

	exp, err := newExporter(config, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))

	logs := plog.NewLogs()
	require.NoError(t, exp.pushLogs(context.Background(), logs))
}

func TestExportErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		responseStatus int
		responseBody   string
		headers        map[string]string
		wantErr        bool
	}{
		{
			name:           "success",
			responseStatus: http.StatusOK,
			wantErr:        false,
		},
		{
			name:           "throttled",
			responseStatus: http.StatusTooManyRequests,
			headers:        map[string]string{"Retry-After": "30"},
			wantErr:        true,
		},
		{
			name:           "service unavailable",
			responseStatus: http.StatusServiceUnavailable,
			wantErr:        true,
		},
		{
			name:           "permanent error",
			responseStatus: http.StatusBadRequest,
			responseBody:   "invalid request",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tt.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tt.responseStatus)
				if tt.responseBody != "" {
					w.Write([]byte(tt.responseBody))
				}
			}))
			defer server.Close()

			config := &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: server.URL,
				},
				Token:             "test-token",
				MetricsDataSource: "metrics_test",
				TracesDataSource:  "traces_test",
				LogsDatasource:    "logs_test",
			}

			exp, err := newExporter(config, exportertest.NewNopSettings(metadata.Type))
			require.NoError(t, err)
			require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))

			traces := ptrace.NewTraces()
			rs := traces.ResourceSpans().AppendEmpty()
			ss := rs.ScopeSpans().AppendEmpty()
			span := ss.Spans().AppendEmpty()
			span.SetName("test-span")
			err = exp.pushTraces(context.Background(), traces)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
