GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    configuration.go
    utils.go
    no_zstd.go
)

END()
