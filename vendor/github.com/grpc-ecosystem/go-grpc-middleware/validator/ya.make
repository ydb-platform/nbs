GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    validator.go
)

GO_TEST_SRCS(validator_test.go)

END()

RECURSE(
    # gotest
)
