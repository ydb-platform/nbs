GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    arn.go
    bucket_region.go
    buffered_read_seeker.go
    doc.go
    download.go
    go_module_metadata.go
    pool.go
    read_seeker_write_to.go
    types.go
    upload.go
    writer_read_from.go
)

GO_TEST_SRCS(
    bucket_region_test.go
    buffered_read_seeker_test.go
    pool_test.go
    types_test.go
    upload_internal_test.go
    writer_read_from_test.go
)

GO_XTEST_SRCS(
    download_test.go
    examples_test.go
    shared_test.go
    upload_test.go
)

IF (OS_LINUX)
    SRCS(
        default_read_seeker_write_to.go
        default_writer_read_from.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        default_read_seeker_write_to.go
        default_writer_read_from.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        default_read_seeker_write_to_windows.go
        default_writer_read_from_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
    internal
)
