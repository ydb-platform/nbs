GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    append.go
    flatten.go
    format.go
    group.go
    multierror.go
    prefix.go
    sort.go
)

GO_TEST_SRCS(
    append_test.go
    flatten_test.go
    format_test.go
    group_test.go
    multierror_test.go
    prefix_test.go
    sort_test.go
)

END()

RECURSE(
    gotest
)
