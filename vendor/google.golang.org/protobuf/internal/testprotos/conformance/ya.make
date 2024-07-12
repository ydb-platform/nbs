GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    conformance.pb.go
    test_messages_proto2.pb.go
    test_messages_proto3.pb.go
)

END()

RECURSE(
    editions
)
