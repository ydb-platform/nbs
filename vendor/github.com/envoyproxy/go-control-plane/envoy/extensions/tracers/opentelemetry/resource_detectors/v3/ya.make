GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dynatrace_resource_detector.pb.go
    dynatrace_resource_detector.pb.validate.go
    environment_resource_detector.pb.go
    environment_resource_detector.pb.validate.go
)

END()
