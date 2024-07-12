GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dubbo_proxy.pb.go
    dubbo_proxy.pb.validate.go
    route.pb.go
    route.pb.validate.go
)

END()
