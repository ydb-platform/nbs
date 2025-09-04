GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    buffer.go
    convert.go
)

GO_TEST_SRCS(
    buffer_test.go
    convert_bench_test.go
    convert_test.go
)

END()

RECURSE(
    gotest
)
