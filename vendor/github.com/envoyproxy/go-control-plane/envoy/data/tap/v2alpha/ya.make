GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    common.pb.go
    common.pb.validate.go
    http.pb.go
    http.pb.validate.go
    transport.pb.go
    transport.pb.validate.go
    wrapper.pb.go
    wrapper.pb.validate.go
)

END()
