GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    addr.go
    agent.go
    attributes.go
    checks.go
    client.go
    errorcode.go
    errors.go
    fingerprint.go
    helpers.go
    integrity.go
    message.go
    stun.go
    textattrs.go
    uattrs.go
    uri.go
    xoraddr.go
)

GO_TEST_SRCS(
    addr_test.go
    agent_test.go
    attributes_test.go
    client_test.go
    errorcode_test.go
    errors_test.go
    fingerprint_test.go
    fuzz_test.go
    helpers_test.go
    iana_test.go
    integrity_test.go
    message_test.go
    rfc5769_test.go
    stun_test.go
    textattrs_test.go
    uattrs_test.go
    uri_test.go
    xoraddr_test.go
)

END()

RECURSE(
    gotest
    internal
)
