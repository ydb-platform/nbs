GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

IF (OS_WINDOWS)
    SRCS(
        swbemservices.go
        wmi.go
    )

    GO_TEST_SRCS(
        swbemservices_test.go
        wmi_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
