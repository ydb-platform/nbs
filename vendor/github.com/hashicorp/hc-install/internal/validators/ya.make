GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    validators.go
)

GO_TEST_SRCS(validators_test.go)

END()

RECURSE(
    gotest
)
