GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    controlplane.go
    e2e.go
    e2e_utils.go
)

GO_TEST_SRCS(e2e_test.go)

END()

RECURSE(
    gotest
)
