package hclogutils_test

import (
    "sort"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/terraform-plugin-log/internal/hclogutils"
)

func TestFieldMapsToArgs(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        maps         []map[string]interface{}
        expectedArgs []interface{}
    }{
        "nil": {
            maps:         nil,
            expectedArgs: nil,
        },
        "map-single": {
            maps: []map[string]interface{}{
                {
                    "map1-key1": "map1-value1",
                    "map1-key2": "map1-value2",
                    "map1-key3": "map1-value3",
                },
            },
            expectedArgs: []interface{}{
                "map1-key1", "map1-value1",
                "map1-key2", "map1-value2",
                "map1-key3", "map1-value3",
            },
        },
        "map-multiple-different-keys": {
            maps: []map[string]interface{}{
                {
                    "map1-key1": "map1-value1",
                    "map1-key2": "map1-value2",
                    "map1-key3": "map1-value3",
                },
                {
                    "map2-key1": "map2-value1",
                    "map2-key2": "map2-value2",
                    "map2-key3": "map2-value3",
                },
            },
            expectedArgs: []interface{}{
                "map1-key1", "map1-value1",
                "map1-key2", "map1-value2",
                "map1-key3", "map1-value3",
                "map2-key1", "map2-value1",
                "map2-key2", "map2-value2",
                "map2-key3", "map2-value3",
            },
        },
        "map-multiple-mixed-keys": {
            maps: []map[string]interface{}{
                {
                    "key1": "map1-value1",
                    "key2": "map1-value2",
                    "key3": "map1-value3",
                },
                {
                    "key4": "map2-value4",
                    "key1": "map2-value1",
                    "key5": "map2-value5",
                },
            },
            expectedArgs: []interface{}{
                "key1", "map2-value1",
                "key2", "map1-value2",
                "key3", "map1-value3",
                "key4", "map2-value4",
                "key5", "map2-value5",
            },
        },
        "map-multiple-overlapping-keys": {
            maps: []map[string]interface{}{
                {
                    "key1": "map1-value1",
                    "key2": "map1-value2",
                    "key3": "map1-value3",
                },
                {
                    "key1": "map2-value1",
                    "key2": "map2-value2",
                    "key3": "map2-value3",
                },
            },
            expectedArgs: []interface{}{
                "key1", "map2-value1",
                "key2", "map2-value2",
                "key3", "map2-value3",
            },
        },
        "map-multiple-overlapping-keys-shallow": {
            maps: []map[string]interface{}{
                {
                    "key1": map[string]interface{}{
                        "submap-key1": "map1-value1",
                        "submap-key2": "map1-value2",
                        "submap-key3": "map1-value3",
                    },
                    "key2": "map1-value2",
                    "key3": "map1-value3",
                },
                {
                    "key1": map[string]interface{}{
                        "submap-key4": "map2-value4",
                        "submap-key5": "map2-value5",
                        "submap-key6": "map2-value6",
                    },
                    "key2": "map2-value2",
                    "key3": "map2-value3",
                },
            },
            expectedArgs: []interface{}{
                "key1", map[string]interface{}{
                    "submap-key4": "map2-value4",
                    "submap-key5": "map2-value5",
                    "submap-key6": "map2-value6",
                },
                "key2", "map2-value2",
                "key3", "map2-value3",
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            got := hclogutils.FieldMapsToArgs(testCase.maps...)

            if len(got)%2 != 0 {
                t.Fatalf("expected even number of key-value fields, got: %v", got)
            }

            if got == nil && testCase.expectedArgs == nil {
                return // sortedGot will return []interface{}{} below, nil is what we want
            }

            // Map retrieval is indeterminate in Go, sort the result first.
            // This logic is only necessary in this testing as its automatically
            // handled in go-hclog.
            gotKeys := make([]string, 0, len(got)/2)
            gotValues := make(map[string]interface{}, len(got)/2)

            for i := 0; i < len(got); i += 2 {
                //nolint:forcetypeassert // Not needed in test of log mapping
                k, v := got[i].(string), got[i+1]
                gotKeys = append(gotKeys, k)
                gotValues[k] = v
            }

            sort.Strings(gotKeys)

            sortedGot := make([]interface{}, 0, len(got))

            for _, k := range gotKeys {
                sortedGot = append(sortedGot, k)
                sortedGot = append(sortedGot, gotValues[k])
            }

            if diff := cmp.Diff(sortedGot, testCase.expectedArgs); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}
