GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    martiantest.go
    transport.go
)

GO_TEST_SRCS(
    martiantest_test.go
    transport_test.go
)

END()

RECURSE(
    gotest
)
