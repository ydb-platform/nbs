GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    archive.go
)

GO_TEST_SRCS(archive_test.go)

END()

RECURSE(
    gotest
)
