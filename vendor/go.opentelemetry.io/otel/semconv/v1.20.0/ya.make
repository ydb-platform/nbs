GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attribute_group.go
    doc.go
    event.go
    exception.go
    http.go
    resource.go
    schema.go
    trace.go
)

END()

RECURSE(
    httpconv
    netconv
)
