GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    parse_req.go
    supported_features.go
)

GO_XTEST_SRCS(parse_req_test.go)

END()

RECURSE(
    gotest
)
