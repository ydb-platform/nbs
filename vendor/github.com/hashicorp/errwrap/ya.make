GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    errwrap.go
)

GO_TEST_SRCS(errwrap_test.go)

END()

RECURSE(
    gotest
)
