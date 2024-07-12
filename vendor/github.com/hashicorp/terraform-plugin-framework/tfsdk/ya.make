GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config.go
    convert.go
    doc.go
    plan.go
    state.go
    value_as.go
    value_from.go
)

GO_TEST_SRCS(
    # convert_test.go
    # value_as_test.go
    # value_from_test.go
)

GO_XTEST_SRCS(
    # config_test.go
    # plan_test.go
    # pointer_test.go
    # state_test.go
)

END()

RECURSE(
    gotest
)
