GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    route.pb.go
    route.pb.validate.go
    thrift_proxy.pb.go
    thrift_proxy.pb.validate.go
)

END()
