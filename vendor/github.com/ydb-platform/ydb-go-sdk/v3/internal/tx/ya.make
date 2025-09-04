GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    context.go
    control.go
    id.go
    settings.go
    transaction.go
)

GO_TEST_SRCS(control_test.go)

END()

RECURSE(
    gotest
)
