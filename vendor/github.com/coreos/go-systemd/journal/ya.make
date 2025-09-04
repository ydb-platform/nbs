GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.0.0-20191104093116-d3cd4ed1dbcf)

SRCS(
    journal.go
)

GO_TEST_SRCS(journal_test.go)

END()

RECURSE(
    gotest
)
