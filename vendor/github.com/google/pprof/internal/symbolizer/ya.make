GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    symbolizer.go
)

GO_TEST_SRCS(symbolizer_test.go)

END()

RECURSE(
    gotest
)
