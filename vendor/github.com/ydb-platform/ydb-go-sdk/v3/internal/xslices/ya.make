GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    delete.go
    diff.go
    filter.go
    keys.go
    map.go
    sort_copy.go
    split.go
    transform.go
    uniq.go
)

GO_TEST_SRCS(
    diff_test.go
    filter_test.go
    keys_test.go
    map_test.go
    sort_copy_test.go
    split_test.go
    transform_test.go
    uniq_test.go
)

END()

RECURSE(
    gotest
)
