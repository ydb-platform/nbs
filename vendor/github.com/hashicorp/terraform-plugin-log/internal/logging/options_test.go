package logging_test

import (
    "os"
    "regexp"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/go-hclog"
    "github.com/hashicorp/terraform-plugin-log/internal/logging"
)

func TestLoggerOptsCopy(t *testing.T) {
    t.Parallel()

    regex1 := regexp.MustCompile("regex1")
    regex2 := regexp.MustCompile("regex2")

    // Populate all fields.
    originalLoggerOpts := logging.LoggerOpts{
        AdditionalLocationOffset:     1,
        Fields:                       map[string]any{"key1": "value1"},
        IncludeLocation:              true,
        IncludeRootFields:            true,
        IncludeTime:                  true,
        Level:                        hclog.Error,
        MaskAllFieldValuesRegexes:    []*regexp.Regexp{regex1},
        MaskAllFieldValuesStrings:    []string{"string1"},
        MaskFieldValuesWithFieldKeys: []string{"string1"},
        MaskMessageRegexes:           []*regexp.Regexp{regex1},
        MaskMessageStrings:           []string{"string1"},
        Name:                         "name1",
        OmitLogWithFieldKeys:         []string{"string1"},
        OmitLogWithMessageRegexes:    []*regexp.Regexp{regex1},
        OmitLogWithMessageStrings:    []string{"string1"},
        Output:                       os.Stdout,
    }

    // Expected LoggerOpts should exactly match original.
    expectedLoggerOpts := logging.LoggerOpts{
        AdditionalLocationOffset:     1,
        Fields:                       map[string]any{"key1": "value1"},
        IncludeLocation:              true,
        IncludeRootFields:            true,
        IncludeTime:                  true,
        Level:                        hclog.Error,
        MaskAllFieldValuesRegexes:    []*regexp.Regexp{regex1},
        MaskAllFieldValuesStrings:    []string{"string1"},
        MaskFieldValuesWithFieldKeys: []string{"string1"},
        MaskMessageRegexes:           []*regexp.Regexp{regex1},
        MaskMessageStrings:           []string{"string1"},
        Name:                         "name1",
        OmitLogWithFieldKeys:         []string{"string1"},
        OmitLogWithMessageRegexes:    []*regexp.Regexp{regex1},
        OmitLogWithMessageStrings:    []string{"string1"},
        Output:                       os.Stdout,
    }

    // Create a copy before modifying the original LoggerOpts. This will be
    // checked against the expected LoggerOpts after modifications.
    copiedLoggerOpts := originalLoggerOpts.Copy()

    // Ensure modifications of original does not effect copy.
    originalLoggerOpts.AdditionalLocationOffset = 2
    originalLoggerOpts.Fields["key2"] = "value2"
    originalLoggerOpts.IncludeLocation = false
    originalLoggerOpts.IncludeRootFields = false
    originalLoggerOpts.IncludeTime = false
    originalLoggerOpts.Level = hclog.Debug
    originalLoggerOpts.MaskAllFieldValuesRegexes = append(originalLoggerOpts.MaskAllFieldValuesRegexes, regex2)
    originalLoggerOpts.MaskAllFieldValuesStrings = append(originalLoggerOpts.MaskAllFieldValuesStrings, "string2")
    originalLoggerOpts.MaskFieldValuesWithFieldKeys = append(originalLoggerOpts.MaskFieldValuesWithFieldKeys, "string2")
    originalLoggerOpts.MaskMessageRegexes = append(originalLoggerOpts.MaskMessageRegexes, regex2)
    originalLoggerOpts.MaskMessageStrings = append(originalLoggerOpts.MaskMessageStrings, "string2")
    originalLoggerOpts.Name = "name2"
    originalLoggerOpts.OmitLogWithFieldKeys = append(originalLoggerOpts.OmitLogWithFieldKeys, "string2")
    originalLoggerOpts.OmitLogWithMessageRegexes = append(originalLoggerOpts.OmitLogWithMessageRegexes, regex2)
    originalLoggerOpts.OmitLogWithMessageStrings = append(originalLoggerOpts.OmitLogWithMessageStrings, "string2")
    originalLoggerOpts.Output = os.Stderr

    // Prevent go-cmp errors.
    cmpOpts := []cmp.Option{
        cmp.Comparer(func(i, j *os.File) bool {
            if i == nil {
                return j == nil
            }

            if j == nil {
                return false
            }

            // Simple comparison test is good enough for our purposes.
            return i.Fd() == j.Fd()
        }),
        cmp.Comparer(func(i, j *regexp.Regexp) bool {
            if i == nil {
                return j == nil
            }

            if j == nil {
                return false
            }

            // Simple comparison test is good enough for our purposes.
            return i.String() == j.String()
        }),
    }

    if diff := cmp.Diff(copiedLoggerOpts, expectedLoggerOpts, cmpOpts...); diff != "" {
        t.Errorf("unexpected difference: %s", diff)
    }
}
