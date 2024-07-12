GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    acknowledgment.go
    cc.go
    feedback_adapter.go
)

GO_TEST_SRCS(feedback_adapter_test.go)

END()

RECURSE(
    gotest
)
