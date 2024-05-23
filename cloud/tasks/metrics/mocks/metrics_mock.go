package mocks

import (
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type sensor interface {
	asMock() *Mock
}

////////////////////////////////////////////////////////////////////////////////

type Mock struct {
	mock.Mock
	ignoreUnknownCalls bool
}

func (o *Mock) On(methodName string, arguments ...interface{}) *mock.Call {
	if o.ignoreUnknownCalls {
		o.ignoreUnknownCalls = false
	}
	return o.Mock.On(methodName, arguments...)
}

func (o *Mock) Called(arguments ...interface{}) mock.Arguments {
	// mock.Mock.Called uses reflection, so we can't just call the parent's
	// Called method.
	if o.ignoreUnknownCalls {
		return nil
	}

	// Here is the same code as in Mock.Called
	// https://github.com/stretchr/testify/blob/master/mock/mock.go#L450
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		panic("Couldn't get the caller information")
	}

	functionPath := runtime.FuncForPC(pc).Name()
	re := regexp.MustCompile(`\.pN\d+_`)
	if re.MatchString(functionPath) {
		functionPath = re.Split(functionPath, -1)[0]
	}

	parts := strings.Split(functionPath, ".")
	functionName := parts[len(parts)-1]
	return o.MethodCalled(functionName, arguments...)
}

////////////////////////////////////////////////////////////////////////////////

type CounterMock struct {
	Mock
}

func (c *CounterMock) Inc() {
	c.Called()
}

func (c *CounterMock) Add(delta int64) {
	c.Called(delta)
}

func (c *CounterMock) asMock() *Mock {
	return &c.Mock
}

////////////////////////////////////////////////////////////////////////////////

type GaugeMock struct {
	Mock
}

func (g *GaugeMock) Set(value float64) {
	g.Called(value)
}

func (g *GaugeMock) Add(value float64) {
	g.Called(value)
}

func (g *GaugeMock) asMock() *Mock {
	return &g.Mock
}

////////////////////////////////////////////////////////////////////////////////

type IntGaugeMock struct {
	Mock
}

func (g *IntGaugeMock) Set(value int64) {
	g.Called(value)
}

func (g *IntGaugeMock) Add(value int64) {
	g.Called(value)
}

func (g *IntGaugeMock) asMock() *Mock {
	return &g.Mock
}

////////////////////////////////////////////////////////////////////////////////

type HistogramMock struct {
	Mock

	Buckets metrics.Buckets
}

func (h *HistogramMock) RecordValue(value float64) {
	h.Called(value)
}

func (h *HistogramMock) asMock() *Mock {
	return &h.Mock
}

////////////////////////////////////////////////////////////////////////////////

type TimerMock struct {
	Mock
}

func (t *TimerMock) RecordDuration(value time.Duration) {
	t.Called(value)
}

func (t *TimerMock) asMock() *Mock {
	return &t.Mock
}

////////////////////////////////////////////////////////////////////////////////

type DurationHistogramMock struct {
	Mock

	Buckets metrics.DurationBuckets
}

func (d *DurationHistogramMock) RecordDuration(value time.Duration) {
	d.Called(value)
}

func (d *DurationHistogramMock) asMock() *Mock {
	return &d.Mock
}

////////////////////////////////////////////////////////////////////////////////

type subregistry struct {
	rootRegistry *RegistryMock
	prefix       string
	tags         map[string]string
}

func (r *subregistry) WithTags(tags map[string]string) metrics.Registry {
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

func (r *subregistry) WithPrefix(prefix string) metrics.Registry {
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

func (r *subregistry) Counter(name string) metrics.Counter {
	return r.rootRegistry.GetCounter(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncCounter(name string, function func() int64) metrics.FuncCounter {
	return nil
}

func (r *subregistry) Gauge(name string) metrics.Gauge {
	return r.rootRegistry.GetGauge(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncGauge(_ string, _ func() float64) metrics.FuncGauge {
	return nil
}

func (r *subregistry) IntGauge(name string) metrics.IntGauge {
	return r.rootRegistry.GetIntGauge(getName(r.prefix, name), r.tags)
}

func (r *subregistry) FuncIntGauge(_ string, _ func() int64) metrics.FuncIntGauge {
	return nil
}

func (r *subregistry) Histogram(
	name string,
	buckets metrics.Buckets,
) metrics.Histogram {

	return r.rootRegistry.GetHistogram(getName(r.prefix, name), r.tags, buckets)
}

func (r *subregistry) Timer(name string) metrics.Timer {
	return r.rootRegistry.GetTimer(getName(r.prefix, name), r.tags)
}

func (r *subregistry) DurationHistogram(
	name string,
	buckets metrics.DurationBuckets,
) metrics.Timer {

	return r.rootRegistry.GetDurationHistogram(
		getName(r.prefix, name),
		r.tags,
		buckets,
	)
}

func (r *subregistry) CounterVec(name string, labels []string) metrics.CounterVec {
	panic("not implemented")
}

func (r *subregistry) GaugeVec(name string, labels []string) metrics.GaugeVec {
	panic("not implemented")
}

func (r *subregistry) IntGaugeVec(name string, labels []string) metrics.IntGaugeVec {
	panic("not implemented")
}

func (r *subregistry) TimerVec(name string, labels []string) metrics.TimerVec {
	panic("not implemented")
}

func (r *subregistry) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	panic("not implemented")
}

func (r *subregistry) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
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
	sensors            map[string]sensor
	sensorsMutex       sync.Mutex
	ignoreUnknownCalls bool
}

func NewRegistryMock() *RegistryMock {
	return &RegistryMock{
		sensors: make(map[string]sensor),
	}
}

func NewIgnoreUnknownCallsRegistryMock() *RegistryMock {
	registryMock := NewRegistryMock()
	registryMock.ignoreUnknownCalls = true
	return registryMock
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
	if r.ignoreUnknownCalls {
		sensor.asMock().ignoreUnknownCalls = true
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
	buckets metrics.Buckets,
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
	buckets metrics.DurationBuckets,
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

func (r *RegistryMock) WithTags(tags map[string]string) metrics.Registry {
	return &subregistry{
		rootRegistry: r,
		tags:         tags,
		prefix:       "",
	}
}

func (r *RegistryMock) WithPrefix(prefix string) metrics.Registry {
	return &subregistry{
		rootRegistry: r,
		tags:         make(map[string]string),
		prefix:       prefix,
	}
}

func (r *RegistryMock) ComposeName(parts ...string) string {
	return strings.Join(parts, ".")
}

func (r *RegistryMock) Counter(name string) metrics.Counter {
	return r.GetCounter(name, make(map[string]string))
}

func (r *RegistryMock) FuncCounter(_ string, _ func() int64) metrics.FuncCounter {
	return nil
}

func (r *RegistryMock) Gauge(name string) metrics.Gauge {
	return r.GetGauge(name, make(map[string]string))
}

func (r *RegistryMock) FuncGauge(_ string, _ func() float64) metrics.FuncGauge {
	return nil
}

func (r *RegistryMock) IntGauge(name string) metrics.IntGauge {
	return r.GetIntGauge(name, make(map[string]string))
}

func (r *RegistryMock) FuncIntGauge(_ string, _ func() int64) metrics.FuncIntGauge {
	return nil
}

func (r *RegistryMock) Histogram(
	name string,
	buckets metrics.Buckets,
) metrics.Histogram {

	return r.GetHistogram(name, make(map[string]string), buckets)
}

func (r *RegistryMock) Timer(name string) metrics.Timer {
	return r.GetTimer(name, make(map[string]string))
}

func (r *RegistryMock) DurationHistogram(
	name string,
	buckets metrics.DurationBuckets,
) metrics.Timer {

	return r.GetDurationHistogram(name, make(map[string]string), buckets)
}

func (r *RegistryMock) CounterVec(name string, labels []string) metrics.CounterVec {
	panic("not implemented")
}

func (r *RegistryMock) GaugeVec(name string, labels []string) metrics.GaugeVec {
	panic("not implemented")
}

func (r *RegistryMock) IntGaugeVec(name string, labels []string) metrics.IntGaugeVec {
	panic("not implemented")
}

func (r *RegistryMock) TimerVec(name string, labels []string) metrics.TimerVec {
	panic("not implemented")
}

func (r *RegistryMock) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	panic("not implemented")
}

func (r *RegistryMock) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
	panic("not implemented")
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that RegistryMock implements Registry.
func assertRegistryMockIsRegistry(arg *RegistryMock) metrics.Registry {
	return arg
}
