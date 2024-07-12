GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    common.go
    environment.go
    version.go
)

END()

RECURSE(
    testing
)
