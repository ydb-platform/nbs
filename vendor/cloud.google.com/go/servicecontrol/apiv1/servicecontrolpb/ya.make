GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    check_error.pb.go
    distribution.pb.go
    http_request.pb.go
    log_entry.pb.go
    metric_value.pb.go
    operation.pb.go
    quota_controller.pb.go
    service_controller.pb.go
)

END()
