GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    conn.go
    errors.go
    isolation.go
    mode.go
    options.go
    rows.go
    tx.go
    tx_fake.go
    valuer.go
)

GO_TEST_SRCS(isolation_test.go)

END()

RECURSE(
    gotest
)
