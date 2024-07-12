GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    exit.go
)

GO_XTEST_SRCS(exit_test.go)

END()

RECURSE(
    gotest
)
