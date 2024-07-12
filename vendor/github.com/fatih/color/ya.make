GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    color.go
    doc.go
)

GO_TEST_SRCS(color_test.go)

IF (OS_WINDOWS)
    SRCS(
        color_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
