GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    changes.go
    filter.go
    index.go
    memdb.go
    schema.go
    txn.go
    watch.go
    watch_few.go
)

GO_TEST_SRCS(
    filter_test.go
    index_test.go
    integ_test.go
    memdb_test.go
    schema_test.go
    txn_test.go
    watch_test.go
)

END()

RECURSE(
    gotest
)
