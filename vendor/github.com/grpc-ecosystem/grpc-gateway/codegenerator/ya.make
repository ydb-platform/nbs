GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.16.0)

SRCS(
    doc.go
    parse_req.go
)

GO_XTEST_SRCS(parse_req_test.go)

END()

RECURSE(
    gotest
)
