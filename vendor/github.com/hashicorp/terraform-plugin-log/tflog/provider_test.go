package tflog_test

import (
    "bytes"
    "context"
    "regexp"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/terraform-plugin-log/internal/loggertest"
    "github.com/hashicorp/terraform-plugin-log/tflog"
)

func TestSetField(t *testing.T) {
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
                    "@module":       "provider",
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
                    "@module":                 "provider",
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
                    "@module":        "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.SetField(ctx, testCase.key, testCase.value)

            tflog.Trace(ctx, testCase.logMessage, testCase.additionalFields...)

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
func TestSetField_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.SetField(originalCtx, "key1", "value1")

    newCtx := tflog.SetField(originalCtx, "key2", "value2")

    tflog.Trace(originalCtx, "original logger")
    tflog.Trace(newCtx, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "provider",
            "key1":     "value1",
            // should not contain key2 field
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "provider",
            "key1":     "value1",
            "key2":     "value2",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestTrace(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":    "provider",
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
                    "@module":    "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)

            tflog.Trace(ctx, testCase.message, testCase.additionalFields...)

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

func TestDebug(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":    "provider",
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
                    "@module":    "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)

            tflog.Debug(ctx, testCase.message, testCase.additionalFields...)

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

func TestInfo(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":    "provider",
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
                    "@module":    "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)

            tflog.Info(ctx, testCase.message, testCase.additionalFields...)

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

func TestWarn(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":    "provider",
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
                    "@module":    "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)

            tflog.Warn(ctx, testCase.message, testCase.additionalFields...)

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

func TestError(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":    "provider",
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
                    "@module":    "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)

            tflog.Error(ctx, testCase.message, testCase.additionalFields...)

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

const testLogMsg = "System FOO has caused error BAR because of incorrectly configured BAZ"

func TestOmitLogWithFieldKeys(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.OmitLogWithFieldKeys(ctx, testCase.omitLogKeys...)

            tflog.Warn(ctx, testCase.msg, testCase.additionalFields...)

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
func TestOmitLogWithFieldKeys_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.OmitLogWithFieldKeys(originalCtx, "key1")

    newCtx := tflog.OmitLogWithFieldKeys(originalCtx, "key2")

    tflog.Trace(originalCtx, "original logger", map[string]any{"key2": "value2"})
    tflog.Trace(newCtx, "new logger", map[string]any{"key1": "value1"})
    tflog.Trace(newCtx, "new logger", map[string]any{"key2": "value2"})

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "provider",
            "key2":     "value2",
        },
        // should omit new logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestOmitLogWithMessageRegexes(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.OmitLogWithMessageRegexes(ctx, testCase.omitLogMatchingRegexp...)

            tflog.Debug(ctx, testCase.msg, testCase.additionalFields...)

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
func TestOmitLogWithMessageRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.OmitLogWithMessageRegexes(originalCtx, regexp.MustCompile("original"))

    newCtx := tflog.OmitLogWithMessageRegexes(originalCtx, regexp.MustCompile("new"))

    tflog.Trace(originalCtx, "original should not be preserved")
    tflog.Trace(originalCtx, "new should be preserved")
    tflog.Trace(newCtx, "new should not be preserved")
    tflog.Trace(newCtx, "original should not be preserved")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "provider",
        },
        // should omit other logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestOmitLogWithMessageStrings(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.OmitLogWithMessageStrings(ctx, testCase.omitLogMatchingString...)

            tflog.Debug(ctx, testCase.msg, testCase.additionalFields...)

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
func TestOmitLogWithMessageStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.OmitLogWithMessageStrings(originalCtx, "original")

    newCtx := tflog.OmitLogWithMessageStrings(originalCtx, "new")

    tflog.Trace(originalCtx, "original should not be preserved")
    tflog.Trace(originalCtx, "new should be preserved")
    tflog.Trace(newCtx, "new should not be preserved")
    tflog.Trace(newCtx, "original should not be preserved")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "provider",
        },
        // should omit other logger entries
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskFieldValuesWithFieldKeys(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, testCase.maskLogKeys...)

            tflog.Error(ctx, testCase.msg, testCase.additionalFields...)

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
func TestMaskFieldValuesWithFieldKeys_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.SetField(originalCtx, "key1", "value1")
    originalCtx = tflog.SetField(originalCtx, "key2", "value2")
    originalCtx = tflog.MaskFieldValuesWithFieldKeys(originalCtx, "key1")

    newCtx := tflog.MaskFieldValuesWithFieldKeys(originalCtx, "key2")

    tflog.Trace(originalCtx, "original logger")
    tflog.Trace(newCtx, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskAllFieldValuesRegexes(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskAllFieldValuesRegexes(ctx, testCase.expressions...)

            tflog.Trace(ctx, testCase.msg, testCase.additionalFields...)

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
func TestMaskAllFieldValuesRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.SetField(originalCtx, "key1", "value1")
    originalCtx = tflog.SetField(originalCtx, "key2", "value2")
    originalCtx = tflog.MaskAllFieldValuesRegexes(originalCtx, regexp.MustCompile("value1"))

    newCtx := tflog.MaskAllFieldValuesRegexes(originalCtx, regexp.MustCompile("value2"))

    tflog.Trace(originalCtx, "original logger")
    tflog.Trace(newCtx, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskAllFieldValuesStrings(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskAllFieldValuesStrings(ctx, testCase.matchingStrings...)

            tflog.Trace(ctx, testCase.msg, testCase.additionalFields...)

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
func TestMaskAllFieldValuesStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.SetField(originalCtx, "key1", "value1")
    originalCtx = tflog.SetField(originalCtx, "key2", "value2")
    originalCtx = tflog.MaskAllFieldValuesStrings(originalCtx, "value1")

    newCtx := tflog.MaskAllFieldValuesStrings(originalCtx, "value2")

    tflog.Trace(originalCtx, "original logger")
    tflog.Trace(newCtx, "new logger")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "original logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "value2",
        },
        {
            "@level":   "trace",
            "@message": "new logger",
            "@module":  "provider",
            "key1":     "***",
            "key2":     "***",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskMessageRegexes(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskMessageRegexes(ctx, testCase.maskLogMatchingRegexp...)

            tflog.Trace(ctx, testCase.msg, testCase.additionalFields...)

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
func TestMaskMessageRegexes_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.MaskMessageRegexes(originalCtx, regexp.MustCompile("original"))

    newCtx := tflog.MaskMessageRegexes(originalCtx, regexp.MustCompile("new"))

    tflog.Trace(originalCtx, "original should be masked")
    tflog.Trace(originalCtx, "new should be preserved")
    tflog.Debug(newCtx, "new should be masked")
    tflog.Debug(newCtx, "original should be masked")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "provider",
        },
        {
            "@level":   "debug",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
        {
            "@level":   "debug",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskMessageStrings(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskMessageStrings(ctx, testCase.maskLogMatchingString...)

            tflog.Info(ctx, testCase.msg, testCase.additionalFields...)

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
func TestMaskMessageStrings_NewContext(t *testing.T) {
    t.Parallel()

    var outputBuffer bytes.Buffer

    originalCtx := context.Background()
    originalCtx = loggertest.ProviderRoot(originalCtx, &outputBuffer)
    originalCtx = tflog.MaskMessageStrings(originalCtx, "original")

    newCtx := tflog.MaskMessageStrings(originalCtx, "new")

    tflog.Trace(originalCtx, "original should be masked")
    tflog.Trace(originalCtx, "new should be preserved")
    tflog.Debug(newCtx, "new should be masked")
    tflog.Debug(newCtx, "original should be masked")

    got, err := loggertest.MultilineJSONDecode(&outputBuffer)

    if err != nil {
        t.Fatalf("unable to read multiple line JSON: %s", err)
    }

    expectedOutput := []map[string]any{
        {
            "@level":   "trace",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
        {
            "@level":   "trace",
            "@message": "new should be preserved",
            "@module":  "provider",
        },
        {
            "@level":   "debug",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
        {
            "@level":   "debug",
            "@message": "*** should be masked",
            "@module":  "provider",
        },
    }

    if diff := cmp.Diff(expectedOutput, got); diff != "" {
        t.Errorf("unexpected new logger output difference: %s", diff)
    }
}

func TestMaskLogRegexes(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskLogRegexes(ctx, testCase.expressions...)

            tflog.Trace(ctx, testCase.msg, testCase.additionalFields...)

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

func TestMaskLogStrings(t *testing.T) {
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
                    "@module":  "provider",
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
            ctx = loggertest.ProviderRoot(ctx, &outputBuffer)
            ctx = tflog.MaskLogStrings(ctx, testCase.matchingStrings...)

            tflog.Trace(ctx, testCase.msg, testCase.additionalFields...)

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
