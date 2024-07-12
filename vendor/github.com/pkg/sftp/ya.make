GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

GO_SKIP_TESTS(
    TestRunLsWithLicensesFileWithOSLookup
    TestRunLsWithExamplesDirectoryWithOSLookup
)

SRCS(
    allocator.go
    attrs.go
    client.go
    conn.go
    ls_formatting.go
    match.go
    packet-manager.go
    packet-typing.go
    packet.go
    pool.go
    release.go
    request-attrs.go
    request-errors.go
    request-example.go
    request-interfaces.go
    request-server.go
    request.go
    server.go
    sftp.go
    stat_posix.go
)

GO_TEST_SRCS(
    allocator_test.go
    attrs_test.go
    client_integration_test.go
    client_test.go
    ls_formatting_test.go
    packet-manager_test.go
    packet_test.go
    request-attrs_test.go
    request-server_test.go
    request_test.go
    server_integration_test.go
    server_test.go
    sftp_test.go
)

GO_XTEST_SRCS(example_test.go)

IF (OS_LINUX)
    SRCS(
        attrs_unix.go
        ls_unix.go
        request-unix.go
        server_statvfs_impl.go
        server_statvfs_linux.go
        server_unix.go
        syscall_good.go
    )

    GO_TEST_SRCS(
        client_integration_linux_test.go
        server_nowindows_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        attrs_unix.go
        ls_unix.go
        request-unix.go
        server_statvfs_darwin.go
        server_statvfs_impl.go
        server_unix.go
        syscall_good.go
    )

    GO_TEST_SRCS(
        client_integration_darwin_test.go
        server_nowindows_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        attrs_stubs.go
        ls_stub.go
        request_windows.go
        server_statvfs_stubs.go
        server_windows.go
        syscall_fixed.go
    )

    GO_TEST_SRCS(server_windows_test.go)
ENDIF()

END()

RECURSE(
    examples
    gotest
    internal
    server_standalone
)
