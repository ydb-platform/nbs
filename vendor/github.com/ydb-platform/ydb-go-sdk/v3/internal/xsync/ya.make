GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    event_broadcast.go
    mutex.go
)

GO_TEST_SRCS(
    event_broadcast_test.go
    mutex_test.go
)

END()

RECURSE(
    gotest
)
