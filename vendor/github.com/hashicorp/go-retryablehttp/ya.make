GO_LIBRARY()

LICENSE(MPL-2.0)

VERSION(v0.7.7)

SRCS(
    cert_error_go120.go
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
