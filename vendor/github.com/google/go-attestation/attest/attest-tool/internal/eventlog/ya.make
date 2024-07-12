GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    TestParseSecureBootLinux
    TestParseSecureBootWindows
)

SRCS(
    eventlog.go
    secureboot.go
)

END()
