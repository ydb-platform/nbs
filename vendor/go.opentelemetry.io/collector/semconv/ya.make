GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestAllSemConvFilesAreCrated)

GO_TEST_SRCS(semconv_test.go)

END()

RECURSE(
    v1.10.0
    v1.11.0
    v1.12.0
    v1.13.0
    v1.16.0
    v1.17.0
    v1.18.0
    v1.5.0
    v1.6.1
    v1.7.0
    v1.8.0
    v1.9.0
)
