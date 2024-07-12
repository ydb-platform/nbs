GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    client.go
    roundtripper.go
)

GO_TEST_SRCS(
    client_test.go
    roundtripper_test.go
)

END()

RECURSE(
    gotest
)
