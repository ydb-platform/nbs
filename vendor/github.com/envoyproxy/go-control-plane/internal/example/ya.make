GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    logger.go
    resource.go
    server.go
)

END()

RECURSE(
    main
)
