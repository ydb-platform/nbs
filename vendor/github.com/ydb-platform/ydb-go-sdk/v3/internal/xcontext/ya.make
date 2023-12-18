GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context_error.go
    context_with_cancel.go
    context_with_timeout.go
    idempotent.go
    local_dc.go
    retry_call.go
    without_deadline.go
)

GO_TEST_SRCS(
    context_error_test.go
    context_with_cancel_test.go
    context_with_timeout_test.go
)

END()

RECURSE(
    gotest
)
