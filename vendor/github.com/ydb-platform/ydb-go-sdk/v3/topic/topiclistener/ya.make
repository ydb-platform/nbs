GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    event_handler.go
    topic_listener.go
)

GO_TEST_SRCS(event_handler_test.go)

END()

RECURSE(
    gotest
)
