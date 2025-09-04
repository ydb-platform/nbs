GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    data.go
    errors.go
    indexed.go
    named.go
    struct.go
    struct_options.go
)

GO_TEST_SRCS(
    indexed_test.go
    named_test.go
    struct_test.go
)

END()

RECURSE(
    gotest
)
