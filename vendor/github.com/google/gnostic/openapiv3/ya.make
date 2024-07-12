GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    OpenAPIv3.go
    OpenAPIv3.pbalias.go
    annotations.pbalias.go
    document.go
)

GO_TEST_SRCS(openapiv3_test.go)

END()

RECURSE(
    gotest
    schema-generator
)
