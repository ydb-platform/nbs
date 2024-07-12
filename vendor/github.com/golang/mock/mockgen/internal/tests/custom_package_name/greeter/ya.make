GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    greeter.go
)

GO_TEST_SRCS(
    greeter_mock_test.go
    greeter_test.go
)

END()

RECURSE(
    gotest
)
