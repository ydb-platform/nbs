GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    rocketmq_proxy.pb.go
    rocketmq_proxy.pb.validate.go
    route.pb.go
    route.pb.validate.go
)

END()
