GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    credentials.go
    error.go
    go_module_metadata.go
    middleware.go
    presign_middleware.go
    smithy.go
    v4a.go
)

END()

RECURSE(
    internal
)
