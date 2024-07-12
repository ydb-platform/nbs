GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

# test uses local mysqld :(

SRCS(
    auth.go
    conn.go
    req.go
    resp.go
    stmt.go
    tls.go
)

GO_TEST_SRCS(
    # client_test.go
)

END()

RECURSE(
    gotest
)
