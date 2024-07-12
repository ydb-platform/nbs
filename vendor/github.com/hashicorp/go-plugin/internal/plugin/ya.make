GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    gen.go
    grpc_broker.pb.go
    grpc_controller.pb.go
    grpc_stdio.pb.go
)

END()
