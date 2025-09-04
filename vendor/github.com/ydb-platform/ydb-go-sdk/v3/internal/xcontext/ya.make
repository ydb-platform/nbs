GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    cancels_quard.go
    context_error.go
    context_with_cancel.go
    context_with_timeout.go
    done.go
    idempotent.go
    local_dc.go
    merge_contexts.go
    retry_call.go
    value_only.go
)

GO_TEST_SRCS(
    cancels_quard_test.go
    context_error_test.go
    context_with_cancel_test.go
    context_with_timeout_test.go
    done_test.go
)

END()

RECURSE(
    gotest
)
