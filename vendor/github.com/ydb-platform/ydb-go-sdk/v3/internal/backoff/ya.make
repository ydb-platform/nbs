GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    backoff.go
    delay.go
    type.go
)

GO_TEST_SRCS(
    backoff_test.go
    delay_test.go
)

END()

RECURSE(
    gotest
)
