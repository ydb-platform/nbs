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

const defaultRetriableErrorsThreshold = 10 * time.Minute

const volumeId1 = "disk-123"
const volumeId2 = "disk-456"
const filesystemId1 = "filesystem-123"

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

func NewTestMonitoring(retriableErrorsDurationThreshold time.Duration) *Monitoring {
	monintoringCfg := MonitoringConfig{
		Port:                             7777,
		Path:                             "/metrics",
		Component:                        "server",
		RetriableErrorsDurationThreshold: retriableErrorsDurationThreshold,
		FilesystemPrefix:                 "filesystem",
	}
	mon := NewMonitoring(&monintoringCfg)
	return mon
}

func TestShouldReportVersion(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
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
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	mon.ReportRequestReceived(volumeId1, "/csi.v1.Controller/ControllerPublishVolume")
	mon.ReportRequestReceived(volumeId1, "/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived(volumeId1, "/csi.v1.Controller/CreateVolume")
	mon.ReportRequestReceived(volumeId1, "/csi.v1.Controller/DeleteVolume")

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 1
Count{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 2
Count{component="server",method="/csi.v1.Controller/DeleteVolume",service="nbs"} 1
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
CriticalRetriableErrors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 0
CriticalRetriableErrors{component="server",method="/csi.v1.Controller/DeleteVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
Errors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 0
Errors{component="server",method="/csi.v1.Controller/DeleteVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 1
InflightCount{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 2
InflightCount{component="server",method="/csi.v1.Controller/DeleteVolume",service="nbs"} 1
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
RetriableErrors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 0
RetriableErrors{component="server",method="/csi.v1.Controller/DeleteVolume",service="nbs"} 0
`)
}

func TestShouldReportErrorsAndSuccess(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	method := "/csi.v1.Controller/CreateVolume"
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method, nil, time.Now(), -1)
	mon.ReportRequestCompleted(volumeId1, method, nil, time.Now(), -1)
	mon.ReportRequestCompleted(volumeId1, method, errors.New("some error"), time.Now(), -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 4
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 1
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 1
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 0
# HELP Success
# TYPE Success counter
Success{component="server",method="/csi.v1.Controller/CreateVolume",service="nbs"} 2
`)
}

func TestShouldReportTimeBuckets(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	method := "/csi.v1.Controller/ControllerPublishVolume"
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method, nil, time.Now(), 7*time.Second)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	assert.Equal(t, err, nil)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 1
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 0
# HELP Success
# TYPE Success counter
Success{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 1
# HELP Time
# TYPE Time histogram
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="0.001"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="0.01"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="0.1"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="1"} 0
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="10"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="60"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="600"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="3600"} 1
Time_bucket{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs",le="+Inf"} 1
Time_sum{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 7
Time_count{component="server",method="/csi.v1.Controller/ControllerPublishVolume",service="nbs"} 1
`)
}

func TestShouldReportRetriableErrors(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	method := "/csi.v1.Node/NodeStageVolume"
	timestamp := time.Now()
	duration := 11 * time.Minute
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp, -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Aborted, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.DeadlineExceeded, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Canceled, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.DeadlineExceeded, ""), timestamp.Add(2*duration), -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 6
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 1
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 4
`)
}

func TestShouldReportMountExpirationTimes(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	mon.ReportExternalFsMountExpirationTimes(map[string]int64{
		"fs-123": 123123,
		"fs-456": 456456,
	})

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP ExternalFsMountExpirationTime
# TYPE ExternalFsMountExpirationTime gauge
ExternalFsMountExpirationTime{component="server",filesystem="fs-123"} 123123
ExternalFsMountExpirationTime{component="server",filesystem="fs-456"} 456456
`)
}

func TestShouldReportRetriableErrosAfterExceedingThresholdForVolume(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	timestamp := time.Now()
	method := "/csi.v1.Node/NodeStageVolume"

	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp, -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(5*time.Minute), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(11*time.Minute), -1)

	mon.ReportRequestReceived(volumeId2, method)
	mon.ReportRequestCompleted(volumeId2, method,
		status.Error(codes.Unavailable, ""), timestamp, -1)
	mon.ReportRequestReceived(volumeId2, method)
	mon.ReportRequestCompleted(volumeId2, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(10*time.Minute), -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 5
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 1
`)

	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(12*time.Minute), -1)
	mon.ReportRequestReceived(volumeId2, method)
	mon.ReportRequestCompleted(volumeId2, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(12*time.Minute), -1)

	response, err = http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 7
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 3
`)

	// send success request to reset errors counter for volume so next error
	// should not increase retriable errors counter
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method, nil,
		timestamp.Add(12*time.Minute), -1)
	mon.ReportRequestReceived(volumeId1, method)
	mon.ReportRequestCompleted(volumeId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(15*time.Minute), -1)

	response, err = http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 9
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 3
# HELP Success
# TYPE Success counter
Success{component="server",method="/csi.v1.Node/NodeStageVolume",service="nbs"} 1
`)
}

func TestShouldReportFilestoreErrors(t *testing.T) {
	mon := NewTestMonitoring(defaultRetriableErrorsThreshold)
	method := "/csi.v1.Node/NodeStageVolume"
	timestamp := time.Now()
	duration := 11 * time.Minute
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.Unavailable, ""), timestamp, -1)
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.Unavailable, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.Aborted, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.DeadlineExceeded, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.Canceled, ""), timestamp.Add(duration), -1)
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.DeadlineExceeded, ""), timestamp.Add(2*duration), -1)

	// fatal error
	mon.ReportRequestReceived(filesystemId1, method)
	mon.ReportRequestCompleted(filesystemId1, method,
		status.Error(codes.FailedPrecondition, ""), timestamp, -1)

	serv := httptest.NewServer(mon.Handler)
	defer serv.Close()

	response, err := http.Get(serv.URL)
	require.NoError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	assert.Equal(t, getResponseBody(response),
		`# HELP Count
# TYPE Count counter
Count{component="server",method="/csi.v1.Node/NodeStageVolume",service="filestore"} 7
# HELP CriticalRetriableErrors
# TYPE CriticalRetriableErrors counter
CriticalRetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="filestore"} 1
# HELP Errors
# TYPE Errors counter
Errors{component="server",method="/csi.v1.Node/NodeStageVolume",service="filestore"} 1
# HELP InflightCount
# TYPE InflightCount gauge
InflightCount{component="server",method="/csi.v1.Node/NodeStageVolume",service="filestore"} 0
# HELP RetriableErrors
# TYPE RetriableErrors counter
RetriableErrors{component="server",method="/csi.v1.Node/NodeStageVolume",service="filestore"} 4
`)
}
