GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generate.go
    secrets.go
    secrets.pb.go
)

END()

RECURSE(
    secretsprovider
)
