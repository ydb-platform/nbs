package tfsdklog

import (
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/go-hclog"
)

func TestRootWouldLog(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        rootLevel  hclog.Level
        checkLevel hclog.Level
        expected   bool
    }{
        "true": {
            rootLevel:  hclog.Trace,
            checkLevel: hclog.Debug,
            expected:   true,
        },
        "false": {
            rootLevel:  hclog.Warn,
            checkLevel: hclog.Debug,
            expected:   false,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            rootLevelMutex.Lock()

            rootLevel = testCase.rootLevel

            rootLevelMutex.Unlock()

            got := rootWouldLog(testCase.checkLevel)

            if diff := cmp.Diff(got, testCase.expected); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}

func TestSubsystemWouldLog(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        subsystemLevel hclog.Level
        checkLevel     hclog.Level
        expected       bool
    }{
        "true": {
            subsystemLevel: hclog.Trace,
            checkLevel:     hclog.Debug,
            expected:       true,
        },
        "false": {
            subsystemLevel: hclog.Warn,
            checkLevel:     hclog.Debug,
            expected:       false,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            subsystemLevelsMutex.Lock()

            subsystemLevels[t.Name()] = testCase.subsystemLevel

            subsystemLevelsMutex.Unlock()

            got := subsystemWouldLog(t.Name(), testCase.checkLevel)

            subsystemLevelsMutex.Lock()

            delete(subsystemLevels, t.Name())

            subsystemLevelsMutex.Unlock()

            if diff := cmp.Diff(got, testCase.expected); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}

func TestWouldLog(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        setLevel   hclog.Level
        checkLevel hclog.Level
        expected   bool
    }{
        "nolevel-nolevel": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.NoLevel,
            expected:   true,
        },
        "nolevel-trace": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Trace,
            expected:   true,
        },
        "nolevel-debug": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Debug,
            expected:   true,
        },
        "nolevel-info": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Info,
            expected:   true,
        },
        "nolevel-warn": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Warn,
            expected:   true,
        },
        "nolevel-error": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "nolevel-off": {
            setLevel:   hclog.NoLevel,
            checkLevel: hclog.Off,
            expected:   false,
        },
        "trace-nolevel": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.NoLevel,
            expected:   false,
        },
        "trace-trace": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Trace,
            expected:   true,
        },
        "trace-debug": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Debug,
            expected:   true,
        },
        "trace-info": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Info,
            expected:   true,
        },
        "trace-warn": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Warn,
            expected:   true,
        },
        "trace-error": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "trace-off": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.Off,
            expected:   false,
        },
        "debug-nolevel": {
            setLevel:   hclog.Trace,
            checkLevel: hclog.NoLevel,
            expected:   false,
        },
        "debug-trace": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Trace,
            expected:   false,
        },
        "debug-debug": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Debug,
            expected:   true,
        },
        "debug-info": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Info,
            expected:   true,
        },
        "debug-warn": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Warn,
            expected:   true,
        },
        "debug-error": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "debug-off": {
            setLevel:   hclog.Debug,
            checkLevel: hclog.Off,
            expected:   false,
        },
        "info-nolevel": {
            setLevel:   hclog.Info,
            checkLevel: hclog.NoLevel,
            expected:   false,
        },
        "info-trace": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Trace,
            expected:   false,
        },
        "info-debug": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Debug,
            expected:   false,
        },
        "info-info": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Info,
            expected:   true,
        },
        "info-warn": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Warn,
            expected:   true,
        },
        "info-error": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "info-off": {
            setLevel:   hclog.Info,
            checkLevel: hclog.Off,
            expected:   false,
        },
        "warn-nolevel": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.NoLevel,
            expected:   false,
        },
        "warn-trace": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Trace,
            expected:   false,
        },
        "warn-debug": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Debug,
            expected:   false,
        },
        "warn-info": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Info,
            expected:   false,
        },
        "warn-warn": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Warn,
            expected:   true,
        },
        "warn-error": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "warn-off": {
            setLevel:   hclog.Warn,
            checkLevel: hclog.Off,
            expected:   false,
        },
        "error-nolevel": {
            setLevel:   hclog.Error,
            checkLevel: hclog.NoLevel,
            expected:   false,
        },
        "error-trace": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Trace,
            expected:   false,
        },
        "error-debug": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Debug,
            expected:   false,
        },
        "error-info": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Info,
            expected:   false,
        },
        "error-warn": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Warn,
            expected:   false,
        },
        "error-error": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Error,
            expected:   true,
        },
        "error-off": {
            setLevel:   hclog.Error,
            checkLevel: hclog.Off,
            expected:   false,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            got := wouldLog(testCase.setLevel, testCase.checkLevel)

            if diff := cmp.Diff(got, testCase.expected); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}
