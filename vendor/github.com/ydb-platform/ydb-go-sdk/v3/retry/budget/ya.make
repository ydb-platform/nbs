GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    budget.go
    errors.go
)

GO_TEST_SRCS(budget_test.go)

END()

RECURSE(
    gotest
)
