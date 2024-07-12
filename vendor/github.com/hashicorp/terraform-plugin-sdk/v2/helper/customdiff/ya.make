GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    compose.go
    computed.go
    condition.go
    doc.go
    force_new.go
    validate.go
)

GO_TEST_SRCS(
    compose_test.go
    computed_test.go
    condition_test.go
    force_new_test.go
    testing_test.go
    validate_test.go
)

END()

RECURSE(
    gotest
)
