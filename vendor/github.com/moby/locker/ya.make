GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    locker.go
)

GO_TEST_SRCS(locker_test.go)

END()

RECURSE(
    gotest
)
