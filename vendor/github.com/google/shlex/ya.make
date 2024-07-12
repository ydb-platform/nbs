GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    shlex.go
)

GO_TEST_SRCS(shlex_test.go)

END()

RECURSE(
    gotest
)
