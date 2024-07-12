GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    clientinfohandler.go
    compression.go
    compressor.go
    confighttp.go
    doc.go
)

GO_TEST_SRCS(
    clientinfohandler_test.go
    compression_test.go
    confighttp_test.go
)

END()

RECURSE(
    gotest
)
