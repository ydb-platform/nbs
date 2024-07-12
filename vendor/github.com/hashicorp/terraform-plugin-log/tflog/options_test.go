package tflog_test

import (
    "bytes"
    "context"
    "strings"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/terraform-plugin-log/internal/loggertest"
    "github.com/hashicorp/terraform-plugin-log/tflog"
)

func testSubsystemTraceHelper(ctx context.Context, message string) {
    tflog.SubsystemTrace(ctx, testSubsystem, message)
}

func TestWithAdditionalLocationOffset(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        additionalLocationOffset int
        logImpl                  func(context.Context)
        expectedOutput           []map[string]interface{}
    }{
        "0-no-helper": {
            additionalLocationOffset: 0,
            logImpl: func(ctx context.Context) {
                tflog.SubsystemTrace(ctx, testSubsystem, "test message")
            },
            expectedOutput: []map[string]interface{}{
                {
                    // Caller line (number after colon) should match
                    // tflog.SubsystemTrace() line in test case implementation.
                    "@caller":  "/tflog/options_test.go:29",
                    "@level":   "trace",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "0-one-helper": {
            additionalLocationOffset: 0,
            logImpl: func(ctx context.Context) {
                testSubsystemTraceHelper(ctx, "test message")
            },
            expectedOutput: []map[string]interface{}{
                {
                    // Caller line (number after colon) should match
                    // tflog.SubsystemTrace() line in testSubsystemTraceHelper
                    // function implementation.
                    "@caller":  "/tflog/options_test.go:15",
                    "@level":   "trace",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
                },
            },
        },
        "1-one-helper": {
            additionalLocationOffset: 1,
            logImpl: func(ctx context.Context) {
                testSubsystemTraceHelper(ctx, "test message")
            },
            expectedOutput: []map[string]interface{}{
                {
                    // Caller line (number after colon) should match
                    // testSubsystemTraceHelper() line in test case
                    // implementation.
                    "@caller":  "/tflog/options_test.go:62",
                    "@level":   "trace",
                    "@message": "test message",
                    "@module":  testSubsystemModule,
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
            ctx = loggertest.ProviderRootWithLocation(ctx, &outputBuffer)
            ctx = tflog.NewSubsystem(ctx, testSubsystem, tflog.WithAdditionalLocationOffset(testCase.additionalLocationOffset))

            testCase.logImpl(ctx)

            got, err := loggertest.MultilineJSONDecode(&outputBuffer)

            if err != nil {
                t.Fatalf("unable to read multiple line JSON: %s", err)
            }

            // Strip non-deterministic caller information up to this package, e.g.
            // /Users/example/src/github.com/hashicorp/terraform-plugin-log/tflog/...
            for _, gotEntry := range got {
                caller, ok := gotEntry["@caller"].(string)

                if !ok {
                    continue
                }

                packageIndex := strings.Index(caller, "/tflog/")

                if packageIndex == -1 {
                    continue
                }

                gotEntry["@caller"] = caller[packageIndex:]
            }

            if diff := cmp.Diff(testCase.expectedOutput, got); diff != "" {
                t.Errorf("unexpected output difference: %s", diff)
            }
        })
    }
}

func TestWithRootFields(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        logMessage      string
        rootFields      map[string]interface{}
        subsystemFields map[string]interface{}
        expectedOutput  []map[string]interface{}
    }{
        "no-root-log-fields": {
            subsystemFields: map[string]interface{}{
                "test-subsystem-key": "test-subsystem-value",
            },
            logMessage: "test message",
            expectedOutput: []map[string]interface{}{
                {
                    "@level":             "trace",
                    "@message":           "test message",
                    "@module":            testSubsystemModule,
                    "test-subsystem-key": "test-subsystem-value",
                },
            },
        },
        "with-root-log-fields": {
            subsystemFields: map[string]interface{}{
                "test-subsystem-key": "test-subsystem-value",
            },
            logMessage: "test message",
            rootFields: map[string]interface{}{
                "test-root-key": "test-root-value",
            },
            expectedOutput: []map[string]interface{}{
                {
                    "@level":             "trace",
                    "@message":           "test message",
                    "@module":            testSubsystemModule,
                    "test-root-key":      "test-root-value",
                    "test-subsystem-key": "test-subsystem-value",
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

            for key, value := range testCase.rootFields {
                ctx = tflog.SetField(ctx, key, value)
            }

            ctx = tflog.NewSubsystem(ctx, testSubsystem, tflog.WithRootFields())

            for key, value := range testCase.subsystemFields {
                ctx = tflog.SubsystemSetField(ctx, testSubsystem, key, value)
            }

            tflog.SubsystemTrace(ctx, testSubsystem, testCase.logMessage)

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
