GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

GO_SKIP_TESTS(
    TestScan
    # Expects $GOROOT/src/encoding/json to exist
)

SRCS(
    build.go
    read.go
    scan.go
)

GO_TEST_SRCS(
    read_test.go
    scan_test.go
)

END()

RECURSE(
    gotest
)
