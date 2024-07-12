GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    symbolz.go
)

GO_TEST_SRCS(symbolz_test.go)

END()

RECURSE(
    gotest
)
