GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    codec.go
    doc.go
    io.go
    schematypes.go
)

GO_TEST_SRCS(
    codec_test.go
    io_test.go
    schematypes_test.go
)

END()

RECURSE(
    gotest
)
