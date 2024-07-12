GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    tryfunc.go
)

GO_TEST_SRCS(tryfunc_test.go)

END()

RECURSE(
    gotest
)
