GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    trace_service.pb.go
    trace_service.pb.gw.go
    trace_service_grpc.pb.go
)

END()
