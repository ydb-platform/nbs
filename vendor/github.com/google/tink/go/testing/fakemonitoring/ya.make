GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    fakemonitoring.go
)

GO_XTEST_SRCS(fakemonitoring_test.go)

END()

RECURSE(
    gotest
)
