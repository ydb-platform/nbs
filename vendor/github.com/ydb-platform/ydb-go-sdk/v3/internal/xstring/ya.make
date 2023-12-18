GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    buffer.go
    convert_go1.20.go
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
