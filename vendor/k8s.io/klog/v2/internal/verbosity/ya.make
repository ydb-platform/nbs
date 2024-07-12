GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    verbosity.go
)

GO_TEST_SRCS(
    helper_test.go
    verbosity_test.go
)

END()

RECURSE(
    gotest
)
