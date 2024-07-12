GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    userdata.go
)

GO_XTEST_SRCS(
    userdata_cli_interop_test.go
    userdata_test.go
)

END()

RECURSE(
    gotest
)
