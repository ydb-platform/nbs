GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bigtable.pb.go
    data.pb.go
    feature_flags.pb.go
    request_stats.pb.go
    response_params.pb.go
)

END()
