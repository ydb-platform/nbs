GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    encoding.go
)

END()

RECURSE(
    cbor
    httpbinding
    json
    xml
)
