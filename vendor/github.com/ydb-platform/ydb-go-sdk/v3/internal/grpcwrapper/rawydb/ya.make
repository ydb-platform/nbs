GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    issues.go
    operation.go
    operation_params.go
    status.go
)

GO_TEST_SRCS(
    issues_test.go
    status_test.go
)

END()

RECURSE(
    gotest
)
