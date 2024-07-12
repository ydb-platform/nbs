package logging_test

import (
    "bytes"
    "os"
    "strings"
    "testing"

    "github.com/pion/logging"
)

func testNoDebugLevel(t *testing.T, logger *logging.DefaultLeveledLogger) {
    var outBuf bytes.Buffer
    logger.WithOutput(&outBuf)

    logger.Debug("this shouldn't be logged")
    if outBuf.Len() > 0 {
        t.Error("Debug was logged when it shouldn't have been")
    }
    logger.Debugf("this shouldn't be logged")
    if outBuf.Len() > 0 {
        t.Error("Debug was logged when it shouldn't have been")
    }
}

func testDebugLevel(t *testing.T, logger *logging.DefaultLeveledLogger) {
    var outBuf bytes.Buffer
    logger.WithOutput(&outBuf)

    dbgMsg := "this is a debug message"
    logger.Debug(dbgMsg)
    if !strings.Contains(outBuf.String(), dbgMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", dbgMsg, outBuf.String())
    }
    logger.Debugf(dbgMsg)
    if !strings.Contains(outBuf.String(), dbgMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", dbgMsg, outBuf.String())
    }
}

func testWarnLevel(t *testing.T, logger *logging.DefaultLeveledLogger) {
    var outBuf bytes.Buffer
    logger.WithOutput(&outBuf)

    warnMsg := "this is a warning message"
    logger.Warn(warnMsg)
    if !strings.Contains(outBuf.String(), warnMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", warnMsg, outBuf.String())
    }
    logger.Warnf(warnMsg)
    if !strings.Contains(outBuf.String(), warnMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", warnMsg, outBuf.String())
    }
}

func testErrorLevel(t *testing.T, logger *logging.DefaultLeveledLogger) {
    var outBuf bytes.Buffer
    logger.WithOutput(&outBuf)

    errMsg := "this is an error message"
    logger.Error(errMsg)
    if !strings.Contains(outBuf.String(), errMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", errMsg, outBuf.String())
    }
    logger.Errorf(errMsg)
    if !strings.Contains(outBuf.String(), errMsg) {
        t.Errorf("Expected to find %q in %q, but didn't", errMsg, outBuf.String())
    }
}

func TestDefaultLoggerFactory(t *testing.T) {
    f := logging.DefaultLoggerFactory{
        Writer:          os.Stdout,
        DefaultLogLevel: logging.LogLevelWarn,
        ScopeLevels: map[string]logging.LogLevel{
            "foo": logging.LogLevelDebug,
        },
    }

    logger := f.NewLogger("baz")
    bazLogger, ok := logger.(*logging.DefaultLeveledLogger)
    if !ok {
        t.Error("Invalid logger type")
    }

    testNoDebugLevel(t, bazLogger)
    testWarnLevel(t, bazLogger)

    logger = f.NewLogger("foo")
    fooLogger, ok := logger.(*logging.DefaultLeveledLogger)
    if !ok {
        t.Error("Invalid logger type")
    }

    testDebugLevel(t, fooLogger)
}

func TestDefaultLogger(t *testing.T) {
    logger := logging.
        NewDefaultLeveledLoggerForScope("test1", logging.LogLevelWarn, os.Stdout)

    testNoDebugLevel(t, logger)
    testWarnLevel(t, logger)
    testErrorLevel(t, logger)
}

func TestSetLevel(t *testing.T) {
    logger := logging.
        NewDefaultLeveledLoggerForScope("testSetLevel", logging.LogLevelWarn, os.Stdout)

    testNoDebugLevel(t, logger)
    logger.SetLevel(logging.LogLevelDebug)
    testDebugLevel(t, logger)
}
