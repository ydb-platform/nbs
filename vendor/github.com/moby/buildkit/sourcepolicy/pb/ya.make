GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generate.go
    json.go
    policy.pb.go
)

GO_TEST_SRCS(json_test.go)

END()

RECURSE(
    gotest
)
