GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    error.go
    state.go
    wait.go
)

GO_TEST_SRCS(
    state_test.go
    wait_test.go
)

END()

RECURSE(
    gotest
)
