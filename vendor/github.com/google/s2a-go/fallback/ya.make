GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    s2a_fallback.go
)

GO_TEST_SRCS(s2a_fallback_test.go)

END()

RECURSE(
    gotest
)
