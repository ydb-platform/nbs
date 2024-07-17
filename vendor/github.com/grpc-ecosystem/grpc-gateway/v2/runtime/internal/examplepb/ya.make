GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    example.pb.go
    non_standard_names.pb.go
    non_standard_names_grpc.pb.go
    proto2.pb.go
    proto3.pb.go
)

END()
