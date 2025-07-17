package monitoring

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func trimTrailingWhitespace(input string) string {
	lines := strings.Split(input, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}

	return strings.Join(lines, "\n")
}

func getResponseBody(response *http.Response) string {
	bodyBytes, err := io.ReadAll(response.Body)
	if err == nil {
		return trimTrailingWhitespace(string(bodyBytes))
	} else {
		return err.Error()
	}
}

func NewTestMonitoring() *Monitoring {
	monintoringCfg := MonitoringConfig{
		Port:      7777,
		Path:      "/metrics",
		Component: "server",
	}
	mon := NewMonitoring(&monintoringCfg)
	return mon
}

func TestShouldReportVersion(t *testing.T) {
	mon := NewTestMonitoring()
	mon.ReportVersion("2.5")

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP version
# TYPE version gauge
version{component="server",revision="2.5"} 1
`)
}

func TestShouldReportInflightAndCount(t *testing.T) {
	mon := NewTestMonitoring()
	mon.ReportRequestReceived("/csi.v1.Controller/ControllerPublishVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/DeleteVolume")

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 1
Count{component="server",method="/csi.v1.Controller/CreateVolume"} 2
Count{component="server",method="/csi.v1.Controller/DeleteVolume"} 1
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 1
InflightCount{component="server",method="/csi.v1.Controller/CreateVolume"} 2
InflightCount{component="server",method="/csi.v1.Controller/DeleteVolume"} 1
`)
}

func TestShouldReportErrorsAndSuccess(t *testing.T) {
	mon := NewTestMonitoring()
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived("/csi.v1.Controller/CreateVolume")
	mon.ReportRequestCompleted("/csi.v1.Controller/CreateVolume", nil, -1)
	mon.ReportRequestCompleted("/csi.v1.Controller/CreateVolume", nil, -1)
	mon.ReportRequestCompleted("/csi.v1.Controller/CreateVolume", errors.New("some error"), -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/CreateVolume"} 4
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Controller/CreateVolume"} 1
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/CreateVolume"} 1
# HELP Success
# TYPE Success counter
Success{component="server",method="/csi.v1.Controller/CreateVolume"} 2
`)
}

func TestShouldReportTimeBuckets(t *testing.T) {
	mon := NewTestMonitoring()
	mon.ReportRequestReceived("/csi.v1.Controller/ControllerPublishVolume")
	mon.ReportRequestCompleted("/csi.v1.Controller/ControllerPublishVolume", nil, 7*time.Second)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 1
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 0
# HELP Success
# TYPE Success counter
Success{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 1
# HELP Time
# TYPE Time histogram
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="0.001"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="0.01"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="0.1"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="1"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="10"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="60"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="600"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="3600"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",le="+Inf"} 1
Time_sum{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 7
Time_count{component="server",method="/csi.v1.Controller/ControllerPublishVolume"} 1
`)
}

func TestShouldReportRetriableErros(t *testing.T) {
	mon := NewTestMonitoring()
	mon.ReportRequestReceived("/csi.v1.Node/NodeStageVolume")
	mon.ReportRequestCompleted("/csi.v1.Node/NodeStageVolume", status.Error(codes.Unavailable, ""), -1)
	mon.ReportRequestReceived("/csi.v1.Node/NodeStageVolume")
	mon.ReportRequestCompleted("/csi.v1.Node/NodeStageVolume", status.Error(codes.Aborted, ""), -1)
	mon.ReportRequestReceived("/csi.v1.Node/NodeStageVolume")
	mon.ReportRequestCompleted("/csi.v1.Node/NodeStageVolume", status.Error(codes.AlreadyExists, ""), -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume"} 3
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume"} 3
`)
}
