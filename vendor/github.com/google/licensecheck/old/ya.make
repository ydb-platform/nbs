GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    data.gen.go
    license.go
    normalize.go
    type_string.go
    urls.go
)

GO_TEST_SRCS(
    coverage_test.go
    license_test.go
    normalize_test.go
    url_test.go
)

END()

RECURSE(
    gotest
)
