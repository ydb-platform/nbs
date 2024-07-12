GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    allocation.go
    allocation_manager.go
    channel_bind.go
    errors.go
    five_tuple.go
    permission.go
)

GO_TEST_SRCS(
    allocation_manager_test.go
    allocation_test.go
    channel_bind_test.go
    five_tuple_test.go
)

END()

RECURSE(
    gotest
)
