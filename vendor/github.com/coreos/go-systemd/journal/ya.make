GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    journal.go
)

GO_TEST_SRCS(journal_test.go)

END()

RECURSE(
    gotest
)
