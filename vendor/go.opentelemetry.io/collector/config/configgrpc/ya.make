GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configgrpc.go
    doc.go
    gzip.go
    wrappedstream.go
)

GO_TEST_SRCS(
    configgrpc_benchmark_test.go
    configgrpc_test.go
    wrappedstream_test.go
)

END()

RECURSE(
    gotest
)
