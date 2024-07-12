GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    quote.go
)

GO_TEST_SRCS(quote_test.go)

END()

RECURSE(
    gotest
)
