GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

PEERDIR(
    contrib/libs/brotli/enc
    contrib/libs/brotli/dec
)

GO_TEST_SRCS(cbrotli_test.go)

IF (CGO_ENABLED)
    CGO_SRCS(
        cgo.go
        reader.go
        writer.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
