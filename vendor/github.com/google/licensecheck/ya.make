GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    data.gen.go
    license.go
    scan.go
    urls.go
)

GO_TEST_SRCS(
    license_test.go
    type_test.go
    url_test.go
)

END()

RECURSE(
    gotest
    internal
    old
)
