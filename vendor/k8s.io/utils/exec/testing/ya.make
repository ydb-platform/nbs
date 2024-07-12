GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    fake_exec.go
)

GO_TEST_SRCS(fake_exec_test.go)

END()

RECURSE(
    gotest
)
