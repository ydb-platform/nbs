GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    metrics_service.pb.go
    metrics_service.pb.gw.go
    metrics_service_grpc.pb.go
)

END()
