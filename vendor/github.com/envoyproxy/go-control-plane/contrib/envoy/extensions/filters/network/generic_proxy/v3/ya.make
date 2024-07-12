GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generic_proxy.pb.go
    generic_proxy.pb.validate.go
    route.pb.go
    route.pb.validate.go
)

END()
