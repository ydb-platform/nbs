GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auth.pb.go
    backend.pb.go
    billing.pb.go
    consumer.pb.go
    context.pb.go
    control.pb.go
    documentation.pb.go
    endpoint.pb.go
    log.pb.go
    logging.pb.go
    monitoring.pb.go
    policy.pb.go
    quota.pb.go
    service.pb.go
    source_info.pb.go
    system_parameter.pb.go
    usage.pb.go
)

END()
