GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    OpenAPIv2.go
    OpenAPIv2.pbalias.go
    document.go
)

GO_TEST_SRCS(
    document_test.go
    openapiv2_test.go
)

END()

RECURSE(
    gotest
)
