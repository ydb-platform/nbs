GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ads.pb.go
    ads.pb.validate.go
    hds.pb.go
    hds.pb.validate.go
    rtds.pb.go
    rtds.pb.validate.go
    sds.pb.go
    sds.pb.validate.go
)

END()
