GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    ngread.go
    ngwrite.go
    pcapng.go
    read.go
    snoop.go
    write.go
)

GO_TEST_SRCS(
    ngread_test.go
    ngwrite_test.go
    read_test.go
    snoop_test.go
    write_test.go
)

IF (OS_LINUX)
    SRCS(
        capture.go
    )

    GO_XTEST_SRCS(capture_test.go)
ENDIF()

END()

RECURSE(
    # gotest
)
