package tinybirdexporter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tinybirdexporter/internal/metadata"
)

func TestPushLogsThrottle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("limit"))
	}))
	defer srv.Close()

	cfg := &Config{Endpoint: srv.URL, Datasource: "ds", Token: "t"}
	exp := newTinybirdExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	exp.client = srv.Client()
	exp.endpoint = srv.URL + "?name=ds"

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty()

	err := exp.pushLogs(context.Background(), logs)
	require.Error(t, err)
	expected := exporterhelper.NewThrottleRetry(fmt.Errorf("tinybird error 429: limit"), time.Second)
	assert.Equal(t, expected, err)
}
