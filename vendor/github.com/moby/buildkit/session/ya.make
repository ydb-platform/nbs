GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    group.go
    grpc.go
    manager.go
    session.go
)

END()

RECURSE(
    content
    filesync
    grpchijack
    secrets
    sshforward
    testutil
)
