GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    repeater.go
)

GO_TEST_SRCS(repeater_test.go)

END()

RECURSE(
    gotest
)
