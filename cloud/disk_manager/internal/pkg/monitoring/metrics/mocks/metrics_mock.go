package mocks

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	core_metrics "github.com/ydb-platform/nbs/library/go/core/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type sensor interface {
	asMock() *mock.Mock
}

////////////////////////////////////////////////////////////////////////////////

type CounterMock struct {
	mock.Mock
}

func (c *CounterMock) Inc() {
	c.Called()
}

func (c *CounterMock) Add(delta int64) {
	c.Called(delta)
}

func (c *CounterMock) asMock() *mock.Mock {
	return &c.Mock
}

////////////////////////////////////////////////////////////////////////////////

type GaugeMock struct {
	mock.Mock
}

func (g *GaugeMock) Set(value float64) {
	g.Called(value)
}

func (g *GaugeMock) Add(value float64) {
	g.Called(value)
}

func (g *GaugeMock) asMock() *mock.Mock {
	return &g.Mock
}

////////////////////////////////////////////////////////////////////////////////

type IntGaugeMock struct {
	mock.Mock
}

func (g *IntGaugeMock) Set(value int64) {
	g.Called(value)
}

func (g *IntGaugeMock) Add(value int64) {
	g.Called(value)
}

func (g *IntGaugeMock) asMock() *mock.Mock {
	return &g.Mock
}

////////////////////////////////////////////////////////////////////////////////

type HistogramMock struct {
	mock.Mock

	Buckets core_metrics.Buckets
}

func (h *HistogramMock) RecordValue(value float64) {
	h.Called(value)
}

func (h *HistogramMock) asMock() *mock.Mock {
	return &h.Mock
}

////////////////////////////////////////////////////////////////////////////////

type TimerMock struct {
	mock.Mock
}

func (t *TimerMock) RecordDuration(value time.Duration) {
	t.Called(value)
}

func (t *TimerMock) asMock() *mock.Mock {
	return &t.Mock
}

////////////////////////////////////////////////////////////////////////////////

type DurationHistogramMock struct {
	mock.Mock

	Buckets core_metrics.DurationBuckets
}

func (d *DurationHistogramMock) RecordDuration(value time.Duration) {
	d.Called(value)
}

func (d *DurationHistogramMock) asMock() *mock.Mock {
	return &d.Mock
}

////////////////////////////////////////////////////////////////////////////////

type subregistry struct {
	rootRegistry *RegistryMock
	prefix       string
	tags         map[string]string
}

func (r *subregistry) WithTags(tags map[string]string) core_metrics.Registry {
	newTags := make(map[string]string)
	for k, v := range r.tags {
		newTags[k] = v
	}
	for k, v := range tags {
		newTags[k] = v
	}
	return &subregistry{
		rootRegistry: r.rootRegistry,
		prefix:       r.prefix,
		tags:         newTags,
	}
}

func (r *subregistry) WithPrefix(prefix string) core_metrics.Registry {
	var newPrefix string
	if len(r.prefix) == 0 {
		newPrefix = prefix
	} else {
		newPrefix = fmt.Sprintf("%v.%v", prefix, r.prefix)
	}
	return &subregistry{
		rootRegistry: r.rootRegistry,
		prefix:       newPrefix,
		tags:         r.tags,
	}
}

func (r *subregistry) ComposeName(parts ...string) string {
	return strings.Join(parts, ".")
}

func getName(prefix string, name string) string {
	if len(prefix) == 0 {
		return name
	}
	return fmt.Sprintf("%v.%v", prefix, name)
}

func (r *subregistry) Counter(name string) core_metrics.Counter {
	return r.rootRegistry.GetCounter(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncCounter(name string, function func() int64) core_metrics.FuncCounter {
	return nil
}

func (r *subregistry) Gauge(name string) core_metrics.Gauge {
	return r.rootRegistry.GetGauge(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncGauge(_ string, _ func() float64) core_metrics.FuncGauge {
	return nil
}

func (r *subregistry) IntGauge(name string) core_metrics.IntGauge {
	return r.rootRegistry.GetIntGauge(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncIntGauge(_ string, _ func() int64) core_metrics.FuncIntGauge {
	return nil
}

func (r *subregistry) Histogram(
	name string,
	buckets core_metrics.Buckets,
) core_metrics.Histogram {

	return r.rootRegistry.GetHistogram(getName(r.prefix, name), r.tags, buckets)
}

func (r *subregistry) Timer(name string) core_metrics.Timer {
	return r.rootRegistry.GetTimer(getName(r.prefix, name), r.tags)
}

func (r *subregistry) DurationHistogram(
	name string,
	buckets core_metrics.DurationBuckets,
) core_metrics.Timer {

	return r.rootRegistry.GetDurationHistogram(
		getName(r.prefix, name),
		r.tags,
		buckets,
	)
}

func (r *subregistry) CounterVec(name string, labels []string) core_metrics.CounterVec {
	panic("not implemented")
}

func (r *subregistry) GaugeVec(name string, labels []string) core_metrics.GaugeVec {
	panic("not implemented")
}

func (r *subregistry) IntGaugeVec(name string, labels []string) core_metrics.IntGaugeVec {
	panic("not implemented")
}

func (r *subregistry) TimerVec(name string, labels []string) core_metrics.TimerVec {
	panic("not implemented")
}

func (r *subregistry) HistogramVec(name string, buckets core_metrics.Buckets, labels []string) core_metrics.HistogramVec {
	panic("not implemented")
}

func (r *subregistry) DurationHistogramVec(name string, buckets core_metrics.DurationBuckets, labels []string) core_metrics.TimerVec {
	panic("not implemented")
}

////////////////////////////////////////////////////////////////////////////////

func createSensorKey(name string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder

	builder.WriteString(name)
	builder.WriteRune('[')
	for _, key := range keys {
		builder.WriteString(fmt.Sprintf("%v=%v,", key, tags[key]))
	}
	builder.WriteRune(']')
	return builder.String()
}

////////////////////////////////////////////////////////////////////////////////

type RegistryMock struct {
	sensors      map[string]sensor
	sensorsMutex sync.Mutex
}

func NewRegistryMock() *RegistryMock {
	return &RegistryMock{
		sensors: make(map[string]sensor),
	}
}

func (r *RegistryMock) getOrNewSensor(
	name string,
	tags map[string]string,
	sensor sensor,
) sensor {
	r.sensorsMutex.Lock()
	defer r.sensorsMutex.Unlock()

	key := createSensorKey(name, tags)

	actualSensor, ok := r.sensors[key]
	if ok {
		return actualSensor
	}
	r.sensors[key] = sensor
	return sensor
}

func (r *RegistryMock) GetCounter(
	name string,
	tags map[string]string,
) *CounterMock {

	return r.getOrNewSensor(name, tags, &CounterMock{}).(*CounterMock)
}

func (r *RegistryMock) GetGauge(
	name string,
	tags map[string]string,
) *GaugeMock {

	return r.getOrNewSensor(name, tags, &GaugeMock{}).(*GaugeMock)
}

func (r *RegistryMock) GetIntGauge(
	name string,
	tags map[string]string,
) *IntGaugeMock {

	return r.getOrNewSensor(name, tags, &IntGaugeMock{}).(*IntGaugeMock)
}

func (r *RegistryMock) GetHistogram(
	name string,
	tags map[string]string,
	buckets core_metrics.Buckets,
) *HistogramMock {

	return r.getOrNewSensor(name, tags, &HistogramMock{
		Buckets: buckets,
	}).(*HistogramMock)
}

func (r *RegistryMock) GetTimer(
	name string,
	tags map[string]string,
) *TimerMock {

	return r.getOrNewSensor(name, tags, &TimerMock{}).(*TimerMock)
}

func (r *RegistryMock) GetDurationHistogram(
	name string,
	tags map[string]string,
	buckets core_metrics.DurationBuckets,
) *TimerMock {

	return r.getOrNewSensor(name, tags, &TimerMock{}).(*TimerMock)
}

func (r *RegistryMock) AssertAllExpectations(t *testing.T) bool {
	r.sensorsMutex.Lock()
	defer r.sensorsMutex.Unlock()

	result := true

	for _, sensor := range r.sensors {
		result = sensor.asMock().AssertExpectations(t) && result
	}

	return result
}

func (r *RegistryMock) WithTags(tags map[string]string) core_metrics.Registry {
	return &subregistry{
		rootRegistry: r,
		tags:         tags,
		prefix:       "",
	}
}

func (r *RegistryMock) WithPrefix(prefix string) core_metrics.Registry {
	return &subregistry{
		rootRegistry: r,
		tags:         make(map[string]string),
		prefix:       prefix,
	}
}

func (r *RegistryMock) ComposeName(parts ...string) string {
	return strings.Join(parts, ".")
}

func (r *RegistryMock) Counter(name string) core_metrics.Counter {
	return r.GetCounter(name, make(map[string]string))
}

func (r *RegistryMock) FuncCounter(_ string, _ func() int64) core_metrics.FuncCounter {
	return nil
}

func (r *RegistryMock) Gauge(name string) core_metrics.Gauge {
	return r.GetGauge(name, make(map[string]string))
}

func (r *RegistryMock) FuncGauge(_ string, _ func() float64) core_metrics.FuncGauge {
	return nil
}

func (r *RegistryMock) IntGauge(name string) core_metrics.IntGauge {
	return r.GetIntGauge(name, make(map[string]string))
}

func (r *RegistryMock) FuncIntGauge(_ string, _ func() int64) core_metrics.FuncIntGauge {
	return nil
}

func (r *RegistryMock) Histogram(
	name string,
	buckets core_metrics.Buckets,
) core_metrics.Histogram {

	return r.GetHistogram(name, make(map[string]string), buckets)
}

func (r *RegistryMock) Timer(name string) core_metrics.Timer {
	return r.GetTimer(name, make(map[string]string))
}

func (r *RegistryMock) DurationHistogram(
	name string,
	buckets core_metrics.DurationBuckets,
) core_metrics.Timer {

	return r.GetDurationHistogram(name, make(map[string]string), buckets)
}

func (r *RegistryMock) CounterVec(name string, labels []string) core_metrics.CounterVec {
	panic("not implemented")
}

func (r *RegistryMock) GaugeVec(name string, labels []string) core_metrics.GaugeVec {
	panic("not implemented")
}

func (r *RegistryMock) IntGaugeVec(name string, labels []string) core_metrics.IntGaugeVec {
	panic("not implemented")
}

func (r *RegistryMock) TimerVec(name string, labels []string) core_metrics.TimerVec {
	panic("not implemented")
}

func (r *RegistryMock) HistogramVec(name string, buckets core_metrics.Buckets, labels []string) core_metrics.HistogramVec {
	panic("not implemented")
}

func (r *RegistryMock) DurationHistogramVec(name string, buckets core_metrics.DurationBuckets, labels []string) core_metrics.TimerVec {
	panic("not implemented")
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that RegistryMock implements Registry.
func assertRegistryMockIsRegistry(arg *RegistryMock) metrics.Registry {
	return arg
}
