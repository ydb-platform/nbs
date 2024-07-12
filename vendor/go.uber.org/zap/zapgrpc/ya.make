GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    zapgrpc.go
)

GO_TEST_SRCS(zapgrpc_test.go)

END()

RECURSE(
    gotest
)
