GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    conn.go
    conn_helpers.go
    connector.go
    context.go
    errors.go
    options.go
    rows.go
    stmt.go
    tx.go
    unwrap.go
)

GO_TEST_SRCS(conn_helpers_test.go)

END()

RECURSE(
    badconn
    common
    gotest
    xquery
    xtable
)
