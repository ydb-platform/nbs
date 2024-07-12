GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    endpoint_errors.go
    resource_request.go
)

END()

RECURSE(
    arn
    s3err
)
