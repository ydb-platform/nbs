GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    route.pb.go
    route.pb.validate.go
    udp_proxy.pb.go
    udp_proxy.pb.validate.go
)

END()
