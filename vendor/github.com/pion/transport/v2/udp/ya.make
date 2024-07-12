GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    batchconn.go
    conn.go
)

GO_TEST_SRCS(conn_test.go)

END()

RECURSE(
    gotest
)
