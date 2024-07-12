GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    record.go
    ticketsender.go
)

GO_TEST_SRCS(
    record_test.go
    ticketsender_test.go
)

END()

RECURSE(
    gotest
    internal
)
