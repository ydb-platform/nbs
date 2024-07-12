GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
)

GO_TEST_SRCS(pfring_test.go)

IF (CGO_ENABLED)
    CGO_SRCS(pfring.go)
ENDIF()

END()

RECURSE(
    gotest
)
