GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    nodes.go
    printer.go
)

GO_TEST_SRCS(printer_test.go)

END()

RECURSE(
    gotest
)
