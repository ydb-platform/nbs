GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    problem.go
    promlint.go
    validation.go
)

GO_XTEST_SRCS(promlint_test.go)

END()

RECURSE(
    gotest
    validations
)
