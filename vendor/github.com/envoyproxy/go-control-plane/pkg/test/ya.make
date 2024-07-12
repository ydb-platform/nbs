GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    server.go
)

END()

RECURSE(
    main
    resource
    v3
)
