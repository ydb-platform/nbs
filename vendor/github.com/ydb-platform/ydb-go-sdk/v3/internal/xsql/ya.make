GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    conn.go
    connector.go
    context.go
    dsn.go
    errors.go
    mode.go
    rows.go
    stmt.go
    tx.go
    tx_fake.go
    unwrap_go1.18.go
    valuer.go
)

GO_TEST_SRCS(
    conn_test.go
    dsn_test.go
)

END()

RECURSE(
    badconn
    gotest
    isolation
)
