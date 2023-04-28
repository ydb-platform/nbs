package client

import (
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func durationToMsec(d time.Duration) int64 {
	return d.Nanoseconds() / int64(time.Millisecond)
}

func durationFromMsec(msec int64) time.Duration {
	return time.Duration(msec * int64(time.Millisecond))
}

func timeToTimestamp(t time.Time) int64 {
	return t.UnixNano() / int64(time.Microsecond)
}

func timeFromTimestamp(usec int64) time.Time {
	sec := usec / int64(time.Second)
	nsec := usec % int64(time.Second)
	return time.Unix(sec, nsec*int64(time.Microsecond))
}

////////////////////////////////////////////////////////////////////////////////

type ticker interface {
	TickChan() <-chan time.Time
	TickProcessed()
	Stop()
}

type tickerFactory func(period time.Duration) ticker

////////////////////////////////////////////////////////////////////////////////

type timeTicker struct {
	Ticker *time.Ticker
}

func (t timeTicker) TickChan() <-chan time.Time {
	return t.Ticker.C
}

func (t timeTicker) TickProcessed() {
}

func (t timeTicker) Stop() {
	t.Ticker.Stop()
}

func newTimeTicker(period time.Duration) timeTicker {
	return timeTicker{
		Ticker: time.NewTicker(period),
	}
}

////////////////////////////////////////////////////////////////////////////////

type fakeTicker struct {
	tickChan          chan time.Time
	tickProcessedChan chan bool
	Stopped           bool
}

func (f *fakeTicker) TickChan() <-chan time.Time {
	return f.tickChan
}

func (f *fakeTicker) TickProcessed() {
	f.tickProcessedChan <- true
}

func (f *fakeTicker) Stop() {
	f.Stopped = true
}

func (f *fakeTicker) SendTick() {
	f.tickChan <- time.Now()
	<-f.tickProcessedChan
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{
		tickChan:          make(chan time.Time, 1),
		tickProcessedChan: make(chan bool, 1),
	}
}

func newFakeTickerFactory(f *fakeTicker) tickerFactory {
	maker := func(period time.Duration) ticker {
		f.Stopped = false
		return f
	}
	return maker
}
