package fieldutils_test

import (
    "sort"
    "testing"

    "github.com/google/go-cmp/cmp"
    "github.com/hashicorp/terraform-plugin-log/internal/fieldutils"
)

func TestMergeFieldMaps(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        input    []map[string]interface{}
        expected map[string]interface{}
    }{
        "nil": {
            input:    nil,
            expected: map[string]interface{}{},
        },
        "empty-maps": {
            input: []map[string]interface{}{
                {},
                {},
                {},
            },
            expected: map[string]interface{}{},
        },
        "single-map": {
            input: []map[string]interface{}{
                {
                    "k1": 123,
                    "k2": "two",
                    "k3": 3.45,
                },
            },
            expected: map[string]interface{}{
                "k1": 123,
                "k2": "two",
                "k3": 3.45,
            },
        },
        "multiple-maps": {
            input: []map[string]interface{}{
                {
                    "k1": 123,
                    "k2": "two",
                    "k3": 3.45,
                },
                {
                    "k4": true,
                    "k5": "five",
                    "k6": 6.78,
                },
            },
            expected: map[string]interface{}{
                "k1": 123,
                "k2": "two",
                "k3": 3.45,
                "k4": true,
                "k5": "five",
                "k6": 6.78,
            },
        },
        "key-collision": {
            input: []map[string]interface{}{
                {
                    "k1": 123,
                    "k2": "two",
                    "k3": 3.45,
                },
                {
                    "k1": true,
                    "k5": "five",
                    "k3": 6.78,
                },
                {
                    "k4": "four",
                },
            },
            expected: map[string]interface{}{
                "k1": true,
                "k2": "two",
                "k3": 6.78,
                "k5": "five",
                "k4": "four",
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            got := fieldutils.MergeFieldMaps(testCase.input...)

            if diff := cmp.Diff(got, testCase.expected); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}

func TestFieldMapsToKeys(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        maps     []map[string]interface{}
        expected []string
    }{
        "nil": {
            maps:     nil,
            expected: nil,
        },
        "map-single": {
            maps: []map[string]interface{}{
                {
                    "map1-key1": "map1-value1",
                    "map1-key2": "map1-value2",
                    "map1-key3": "map1-value3",
                },
            },
            expected: []string{
                "map1-key1",
                "map1-key2",
                "map1-key3",
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
            expected: []string{
                "map1-key1",
                "map1-key2",
                "map1-key3",
                "map2-key1",
                "map2-key2",
                "map2-key3",
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
            expected: []string{
                "key1",
                "key2",
                "key3",
                "key4",
                "key5",
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
            expected: []string{
                "key1",
                "key2",
                "key3",
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
                    "key3": "map2-value3",
                    "key2": "map2-value2",
                },
            },
            expected: []string{
                "key1",
                "key2",
                "key3",
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            got := fieldutils.FieldMapsToKeys(testCase.maps...)

            sort.Strings(got)

            if diff := cmp.Diff(got, testCase.expected); diff != "" {
                t.Errorf("unexpected difference: %s", diff)
            }
        })
    }
}
