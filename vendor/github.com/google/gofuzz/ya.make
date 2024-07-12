GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    fuzz.go
)

GO_TEST_SRCS(fuzz_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    bytesource
    gotest
)
