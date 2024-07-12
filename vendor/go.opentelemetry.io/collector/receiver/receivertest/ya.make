GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    contract_checker.go
    nop_receiver.go
)

GO_TEST_SRCS(
    contract_checker_test.go
    nop_receiver_test.go
)

END()

RECURSE(
    gotest
)
