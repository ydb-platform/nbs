GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    env.go
)

GO_TEST_SRCS(env_test.go)

END()

RECURSE(
    gotest
)
