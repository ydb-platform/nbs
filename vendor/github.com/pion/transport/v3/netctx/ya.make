GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    conn.go
    packetconn.go
    pipe.go
)

GO_TEST_SRCS(
    conn_test.go
    packetconn_test.go
)

END()

RECURSE(
    gotest
)
