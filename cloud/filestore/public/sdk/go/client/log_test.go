package client

import (
	"log"
	"log/syslog"
	"strings"
	"testing"
)

func TestLogWithNilLoggerCreation(t *testing.T) {
	log := NewLog(nil, LOG_DEBUG)
	logger := log.Logger(LOG_DEBUG)
	if logger != nil {
		t.Error("logger is not nil although it should be")
	}
}

func TestSysLogWithNilWriterCreation(t *testing.T) {
	log := NewSysLog(nil, LOG_DEBUG)
	logger := log.Logger(LOG_DEBUG)
	if logger != nil {
		t.Error("logger is not nil although it should be")
	}
}

func TestLoggerCreationPerLevel(t *testing.T) {
	var bld strings.Builder
	l := NewLog(log.New(&bld, "logger: ", log.Lshortfile), LOG_INFO)

	logger := l.Logger(LOG_DEBUG)
	if logger != nil {
		t.Error("logger is not nil although it should be")
	}

	levels := []LogLevel{LOG_INFO, LOG_WARN, LOG_ERROR}
	for _, level := range levels {
		logger = l.Logger(level)
		if logger == nil {
			t.Error("logger is nil although it shouldn't be")
		}
	}
}

func TestSysLoggerCreationPerLevel(t *testing.T) {
	writer, err := syslog.New(syslog.LOG_NOTICE, "")
	if err != nil {
		t.Error(err)
	}

	s := NewSysLog(writer, LOG_INFO)

	logger := s.Logger(LOG_DEBUG)
	if logger != nil {
		t.Error("logger is not nil although it should be")
	}

	levels := []LogLevel{LOG_INFO, LOG_WARN, LOG_ERROR}
	for _, level := range levels {
		logger = s.Logger(level)
		if logger == nil {
			t.Error("logger is nil although it shouldn't be")
		}
	}
}
