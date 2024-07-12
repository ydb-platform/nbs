GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    gob.go
    iterator.go
    ops.go
    rules.go
    set.go
)

GO_TEST_SRCS(
    ops_test.go
    rules_test.go
)

END()

RECURSE(
    gotest
)
