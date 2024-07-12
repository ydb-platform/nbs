GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bytesid.go
    spanid.go
    traceid.go
)

GO_TEST_SRCS(
    spanid_test.go
    traceid_test.go
)

END()

RECURSE(
    gotest
    protogen
)
