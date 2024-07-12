GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bitwise.go
    byteorder.go
    connlimit.go
    counter.go
    ct.go
    dup.go
    dynset.go
    expr.go
    exthdr.go
    fib.go
    flow_offload.go
    hash.go
    immediate.go
    limit.go
    log.go
    lookup.go
    match.go
    nat.go
    notrack.go
    numgen.go
    objref.go
    payload.go
    queue.go
    quota.go
    range.go
    redirect.go
    reject.go
    rt.go
    socket.go
    target.go
    tproxy.go
    verdict.go
)

GO_TEST_SRCS(
    bitwise_test.go
    exthdr_test.go
    match_test.go
    meta_test.go
    socket_test.go
    target_test.go
)

END()

RECURSE(
    gotest
)
