GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    logs_service.pb.go
    logs_service.pb.gw.go
    logs_service_grpc.pb.go
)

END()
