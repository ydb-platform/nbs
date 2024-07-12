GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    scanner.go
)

GO_TEST_SRCS(scanner_test.go)

END()

RECURSE(
    gotest
)
