GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    ifaddr.go
    ifaddrs.go
    ifattr.go
    ipaddr.go
    ipaddrs.go
    ipv4addr.go
    ipv6addr.go
    rfc.go
    route_info.go
    sockaddr.go
    sockaddrs.go
    unixsock.go
)

GO_TEST_SRCS(route_info_test.go)

GO_XTEST_SRCS(
    ifaddr_test.go
    ifaddrs_test.go
    ifattr_test.go
    ipaddr_test.go
    ipaddrs_test.go
    ipv4addr_test.go
    ipv6addr_test.go
    rfc_test.go
    sockaddr_test.go
    sockaddrs_test.go
    unixsock_test.go
)

IF (OS_LINUX)
    SRCS(
        route_info_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        route_info_bsd.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        route_info_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
