GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    match.go
)

GO_XTEST_SRCS(match_test.go)

END()

RECURSE(
    gotest
)
