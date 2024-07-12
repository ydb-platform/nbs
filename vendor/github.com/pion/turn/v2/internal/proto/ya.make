GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    addr.go
    chandata.go
    chann.go
    connection_id.go
    data.go
    dontfrag.go
    evenport.go
    lifetime.go
    peeraddr.go
    proto.go
    relayedaddr.go
    reqfamily.go
    reqtrans.go
    rsrvtoken.go
)

GO_TEST_SRCS(
    addr_test.go
    chandata_test.go
    chann_test.go
    chrome_test.go
    data_test.go
    dontfrag_test.go
    evenport_test.go
    fuzz_test.go
    lifetime_test.go
    peeraddr_test.go
    proto_test.go
    relayedaddr_test.go
    reqfamily_test.go
    reqtrans_test.go
    rsrvtoken_test.go
)

END()

RECURSE(
    gotest
)
