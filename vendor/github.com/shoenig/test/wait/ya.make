GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    wait.go
)

GO_TEST_SRCS(wait_test.go)

END()

RECURSE(
    gotest
)
