GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    audit_context.pb.go
)

END()

RECURSE(
    attribute_context
)
