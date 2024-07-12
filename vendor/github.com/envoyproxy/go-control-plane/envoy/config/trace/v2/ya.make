GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    datadog.pb.go
    datadog.pb.validate.go
    dynamic_ot.pb.go
    dynamic_ot.pb.validate.go
    http_tracer.pb.go
    http_tracer.pb.validate.go
    lightstep.pb.go
    lightstep.pb.validate.go
    opencensus.pb.go
    opencensus.pb.validate.go
    service.pb.go
    service.pb.validate.go
    trace.pb.go
    trace.pb.validate.go
    zipkin.pb.go
    zipkin.pb.validate.go
)

END()
