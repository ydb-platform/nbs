GO_LIBRARY()

LICENSE(Apache-2.0)

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
