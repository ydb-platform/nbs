GO_TEST_FOR(vendor/github.com/grpc-ecosystem/go-grpc-middleware/logging/kit)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    TestKitClientSuite
    TestKitClientOverrideSuite
    TestKitPayloadSuite
    TestKitLoggingSuite
    TestKitLoggingOverrideSuite
    TestKitServerOverrideSuppressedSuite
)

END()
