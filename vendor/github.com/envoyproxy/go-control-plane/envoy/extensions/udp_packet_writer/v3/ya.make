GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    udp_default_writer_factory.pb.go
    udp_default_writer_factory.pb.validate.go
    udp_gso_batch_writer_factory.pb.go
    udp_gso_batch_writer_factory.pb.validate.go
)

END()
