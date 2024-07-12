package tfsdklog_test

import (
    "bytes"
    "context"
    "regexp"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/go-hclog"
    "github.com/hashicorp/terraform-plugin-log/internal/loggertest"
    "github.com/hashicorp/terraform-plugin-log/tfsdklog"
)

const (
    testSubsystem       = "test_subsystem"
    testSubsystemModule = "sdk." + testSubsystem
)

func BenchmarkSubsystemTraceDisabled(b *testing.B) {
    benchmarkSubsystemTrace(b, hclog.Debug)
}

func BenchmarkSubsystemTraceEnabled(b *testing.B) {
    benchmarkSubsystemTrace(b, hclog.Trace)
}

func benchmarkSubsystemTrace(b *testing.B, setLevel hclog.Level) {
    var outputBuffer bytes.Buffer

    ctx := context.Background()
    ctx = loggertest.SDKRoot(ctx, &outputBuffer)
    ctx = tfsdklog.NewSubsystem(ctx, b.Name(), tfsdklog.WithLevel(setLevel))

    for n := 0; n < b.N; n++ {
        tfsdklog.SubsystemTrace(ctx, b.Name(), "test message")
    }
}

func TestSubsystemSetField(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        key              string
        value            interface{}
        logMessage       string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-log-fields": {
            key:        "test-with-key",
            value:      "test-with-value",
            logMessage: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":        "trace",
                    "@message":      "test message",
                    "@module":       testSubsystemModule,
                    "test-with-key": "test-with-value",
                },
            },
        },
        "mismatched-with-field": {
            key:        "unfielded-test-with-key",
            logMessage: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":                  "trace",
                    "@message":                "test message",
                    "@module":                 testSubsystemModule,
                    "unfielded-test-with-key": nil,
                },
            },
        },
        "with-and-log-fields": {
            key:        "test-with-key",
            value:      "test-with-value",
            logMessage: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-log-key-1": "test-log-value-1",
                    "test-log-key-2": "test-log-value-2",
                    "test-log-key-3": "test-log-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":         "trace",
                    "@message":       "test message",
                    "@module":        testSubsystemModule,
                    "test-log-key-1": "test-log-value-1",
                    "test-log-key-2": "test-log-value-2",
                    "test-log-key-3": "test-log-value-3",
                    "test-with-key":  "test-with-value",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemSetField(ctx, testSubsystem, testCase.key, testCase.value)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.logMessage, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemSetField_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key1", "value1")

    newCtx := tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key2", "value2")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original logger")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "value1",
            // should not contain key2 field
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "value1",
            "key2":     "value2",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemTrace(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        message          string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-fields": {
            message: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "fields-single-map": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "trace",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
        },
        "fields-multiple-maps": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1-map1",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                },
                {
                    "test-key-4": "test-value-4-map2",
                    "test-key-1": "test-value-1-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "trace",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1-map2",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                    "test-key-4": "test-value-4-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.message, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemDebug(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        message          string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-fields": {
            message: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "debug",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "fields-single-map": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "debug",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
        },
        "fields-multiple-maps": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1-map1",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                },
                {
                    "test-key-4": "test-value-4-map2",
                    "test-key-1": "test-value-1-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "debug",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1-map2",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                    "test-key-4": "test-value-4-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)

            tfsdklog.SubsystemDebug(ctx, testSubsystem, testCase.message, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemInfo(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        message          string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-fields": {
            message: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "info",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "fields-single-map": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "info",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
        },
        "fields-multiple-maps": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1-map1",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                },
                {
                    "test-key-4": "test-value-4-map2",
                    "test-key-1": "test-value-1-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "info",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1-map2",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                    "test-key-4": "test-value-4-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)

            tfsdklog.SubsystemInfo(ctx, testSubsystem, testCase.message, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemWarn(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        message          string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-fields": {
            message: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "warn",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "fields-single-map": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "warn",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
        },
        "fields-multiple-maps": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1-map1",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                },
                {
                    "test-key-4": "test-value-4-map2",
                    "test-key-1": "test-value-1-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "warn",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1-map2",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                    "test-key-4": "test-value-4-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)

            tfsdklog.SubsystemWarn(ctx, testSubsystem, testCase.message, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemError(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        message          string
        additionalFields []map[string]interface{}
        expectedOutput   []map[string]interface{}
    }{
        "no-fields": {
            message: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "error",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "fields-single-map": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "error",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1",
                    "test-key-2": "test-value-2",
                    "test-key-3": "test-value-3",
                },
            },
        },
        "fields-multiple-maps": {
            message: "test message",
            additionalFields: []map[string]interface{}{
                {
                    "test-key-1": "test-value-1-map1",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                },
                {
                    "test-key-4": "test-value-4-map2",
                    "test-key-1": "test-value-1-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":     "error",
                    "@message":   "test message",
                    "@module":    testSubsystemModule,
                    "test-key-1": "test-value-1-map2",
                    "test-key-2": "test-value-2-map1",
                    "test-key-3": "test-value-3-map1",
                    "test-key-4": "test-value-4-map2",
                    "test-key-5": "test-value-5-map2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)

            tfsdklog.SubsystemError(ctx, testSubsystem, testCase.message, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemOmitLogWithFieldKeys(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        omitLogKeys      []string
        expectedOutput   []map[string]interface{}
    }{
        "no-omission": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogKeys: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "warn",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-key-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogKeys: []string{"k3", "K3"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "warn",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "omit-log-by-key": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogKeys:    []string{"k1"},
            expectedOutput: nil,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemOmitLogWithFieldKeys(ctx, testSubsystem, testCase.omitLogKeys...)

            tfsdklog.SubsystemWarn(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemOmitLogWithFieldKeys_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemOmitLogWithFieldKeys(originalCtx, testSubsystem, "key1")

    newCtx := tfsdklog.SubsystemOmitLogWithFieldKeys(originalCtx, testSubsystem, "key2")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original logger", map[string]any{"key2": "value2"})
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger", map[string]any{"key1": "value1"})
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger", map[string]any{"key2": "value2"})

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "sdk.test_subsystem",
            "key2":     "value2",
        },
        // should omit new logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemOmitLogWithMessageRegexes(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg                   string
        additionalFields      []map[string]interface{}
        omitLogMatchingRegexp []*regexp.Regexp
        expectedOutput        []map[string]interface{}
    }{
        "no-omission": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingRegexp: []*regexp.Regexp{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "debug",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingRegexp: []*regexp.Regexp{regexp.MustCompile("(?i)BaAnAnA")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "debug",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "omit-log-matching-regexp": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingRegexp: []*regexp.Regexp{regexp.MustCompile("BAZ$")},
            expectedOutput:        nil,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemOmitLogWithMessageRegexes(ctx, testSubsystem, testCase.omitLogMatchingRegexp...)

            tfsdklog.SubsystemDebug(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemOmitLogWithMessageRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemOmitLogWithMessageRegexes(originalCtx, testSubsystem, regexp.MustCompile("original"))

    newCtx := tfsdklog.SubsystemOmitLogWithMessageRegexes(originalCtx, testSubsystem, regexp.MustCompile("new"))

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original should not be preserved")
    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "new should be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new should not be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "original should not be preserved")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "sdk.test_subsystem",
        },
        // should omit other logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemOmitLogWithMessageStrings(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg                   string
        additionalFields      []map[string]interface{}
        omitLogMatchingString []string
        expectedOutput        []map[string]interface{}
    }{
        "no-omission": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingString: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "debug",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingString: []string{"BaAnAnA"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "debug",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "omit-log-matching-string": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            omitLogMatchingString: []string{"BAZ"},
            expectedOutput:        nil,
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemOmitLogWithMessageStrings(ctx, testSubsystem, testCase.omitLogMatchingString...)

            tfsdklog.SubsystemDebug(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemOmitLogWithMessageStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemOmitLogWithMessageStrings(originalCtx, testSubsystem, "original")

    newCtx := tfsdklog.SubsystemOmitLogWithMessageStrings(originalCtx, testSubsystem, "new")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original should not be preserved")
    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "new should be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new should not be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "original should not be preserved")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "sdk.test_subsystem",
        },
        // should omit other logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskFieldValuesWithFieldKeys(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        maskLogKeys      []string
        expectedOutput   []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogKeys: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "error",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-key-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogKeys: []string{"k3", "K3"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "error",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-log-by-key": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogKeys: []string{"k1", "k2", "k3"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "error",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "***",
                    "k2":       "***",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskFieldValuesWithFieldKeys(ctx, testSubsystem, testCase.maskLogKeys...)

            tfsdklog.SubsystemError(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemMaskFieldValuesWithFieldKeys_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key1", "value1")
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key2", "value2")
    originalCtx = tfsdklog.SubsystemMaskFieldValuesWithFieldKeys(originalCtx, testSubsystem, "key1")

    newCtx := tfsdklog.SubsystemMaskFieldValuesWithFieldKeys(originalCtx, testSubsystem, "key2")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original logger")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskAllFieldValuesRegexes(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        expressions      []*regexp.Regexp
        expectedOutput   []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            expressions: []*regexp.Regexp{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            expressions: []*regexp.Regexp{regexp.MustCompile("v3"), regexp.MustCompile("v4")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-matching-regexp": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1 plus text",
                    "k2": "v2 more text",
                },
            },
            expressions: []*regexp.Regexp{regexp.MustCompile("v1|v2")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "*** plus text",
                    "k2":       "*** more text",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskAllFieldValuesRegexes(ctx, testSubsystem, testCase.expressions...)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemMaskAllFieldValuesRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key1", "value1")
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key2", "value2")
    originalCtx = tfsdklog.SubsystemMaskAllFieldValuesRegexes(originalCtx, testSubsystem, regexp.MustCompile("value1"))

    newCtx := tfsdklog.SubsystemMaskAllFieldValuesRegexes(originalCtx, testSubsystem, regexp.MustCompile("value2"))

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original logger")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskAllFieldValuesStrings(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        matchingStrings  []string
        expectedOutput   []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            matchingStrings: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            matchingStrings: []string{"v3", "v4"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-matching-strings": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1 plus text",
                    "k2": "v2 more text",
                },
            },
            matchingStrings: []string{"v1", "v2"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "*** plus text",
                    "k2":       "*** more text",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskAllFieldValuesStrings(ctx, testSubsystem, testCase.matchingStrings...)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemMaskAllFieldValuesStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key1", "value1")
    originalCtx = tfsdklog.SubsystemSetField(originalCtx, testSubsystem, "key2", "value2")
    originalCtx = tfsdklog.SubsystemMaskAllFieldValuesStrings(originalCtx, testSubsystem, "value1")

    newCtx := tfsdklog.SubsystemMaskAllFieldValuesStrings(originalCtx, testSubsystem, "value2")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original logger")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "sdk.test_subsystem",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskMessageRegexes(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg                   string
        additionalFields      []map[string]interface{}
        maskLogMatchingRegexp []*regexp.Regexp
        expectedOutput        []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingRegexp: []*regexp.Regexp{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingRegexp: []*regexp.Regexp{regexp.MustCompile("(?i)BaAnAnA")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-log-matching-regexp": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingRegexp: []*regexp.Regexp{regexp.MustCompile("BAZ$")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured ***",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskMessageRegexes(ctx, testSubsystem, testCase.maskLogMatchingRegexp...)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemMaskMessageRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemMaskMessageRegexes(originalCtx, testSubsystem, regexp.MustCompile("original"))

    newCtx := tfsdklog.SubsystemMaskMessageRegexes(originalCtx, testSubsystem, regexp.MustCompile("new"))

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original should be masked")
    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "new should be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new should be masked")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "original should be masked")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskMessageStrings(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg                   string
        additionalFields      []map[string]interface{}
        maskLogMatchingString []string
        expectedOutput        []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingString: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "info",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingString: []string{"BaAnAnA"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "info",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-log-matching-string": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            maskLogMatchingString: []string{"incorrectly configured BAZ"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "info",
                    "@message": "System FOO has caused error BAR because of ***",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskMessageStrings(ctx, testSubsystem, testCase.maskLogMatchingString...)

            tfsdklog.SubsystemInfo(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

// Reference: https://github.com/hashicorp/terraform-plugin-log/issues/131
func TestSubsystemMaskMessageStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.SDKRoot(originalCtx, &outputBuffer)
    originalCtx = tfsdklog.NewSubsystem(originalCtx, testSubsystem)
    originalCtx = tfsdklog.SubsystemMaskMessageStrings(originalCtx, testSubsystem, "original")

    newCtx := tfsdklog.SubsystemMaskMessageStrings(originalCtx, testSubsystem, "new")

    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "original should be masked")
    tfsdklog.SubsystemTrace(originalCtx, testSubsystem, "new should be preserved")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "new should be masked")
    tfsdklog.SubsystemTrace(newCtx, testSubsystem, "original should be masked")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "sdk.test_subsystem",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestSubsystemMaskLogRegexes(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        expressions      []*regexp.Regexp
        expectedOutput   []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            expressions: []*regexp.Regexp{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            expressions: []*regexp.Regexp{regexp.MustCompile("v3"), regexp.MustCompile("v4"), regexp.MustCompile("(?i)BaAnAnA")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-matching-regexp": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1 plus text",
                    "k2": "v2 more text",
                },
            },
            expressions: []*regexp.Regexp{regexp.MustCompile("v1|v2"), regexp.MustCompile("FOO|BAR|BAZ")},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System *** has caused error *** because of incorrectly configured ***",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "*** plus text",
                    "k2":       "*** more text",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskLogRegexes(ctx, testSubsystem, testCase.expressions...)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestSubsystemMaskLogStrings(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        msg              string
        additionalFields []map[string]interface{}
        matchingStrings  []string
        expectedOutput   []map[string]interface{}
    }{
        "no-masking": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            matchingStrings: []string{},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "no-matches": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1",
                    "k2": "v2",
                },
            },
            matchingStrings: []string{"v3", "v4"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System FOO has caused error BAR because of incorrectly configured BAZ",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "v1",
                    "k2":       "v2",
                },
            },
        },
        "mask-matching-strings": {
            msg: testLogMsg,
            additionalFields: []map[string]interface{}{
                {
                    "k1": "v1 plus text",
                    "k2": "v2 more text",
                },
            },
            matchingStrings: []string{"v1", "v2", "FOO", "BAR", "BAZ"},
            expectedOutput: []map[string]interface{}{
                {
                    "@level":   "trace",
                    "@message": "System *** has caused error *** because of incorrectly configured ***",
                    "@module":  "sdk.test_subsystem",
                    "k1":       "*** plus text",
                    "k2":       "*** more text",
                },
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            var outputBuffer bytes.Buffer

            ctx := context.Background()
            ctx = loggertest.SDKRoot(ctx, &outputBuffer)
            ctx = tfsdklog.NewSubsystem(ctx, testSubsystem)
            ctx = tfsdklog.SubsystemMaskLogStrings(ctx, testSubsystem, testCase.matchingStrings...)

            tfsdklog.SubsystemTrace(ctx, testSubsystem, testCase.msg, testCase.additionalFields...)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)
            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}
