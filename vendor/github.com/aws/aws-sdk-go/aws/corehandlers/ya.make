GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    awsinternal.go
    handlers.go
    param_validator.go
    user_agent.go
)

GO_TEST_SRCS(user_agent_test.go)

GO_XTEST_SRCS(
    handlers_1_10_test.go
    handlers_test.go
    param_validator_test.go
)

END()

RECURSE(
    # gotest
)
