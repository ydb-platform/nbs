GO_LIBRARY()

LICENSE(Apache-2.0)

IF(GOSTD_VERSION == 1.19)
    SRCS(fixup_go119.go)
ELSE()
    SRCS(fixup_go118.go)
ENDIF()

SRCS(
    doc.go
    exec.go
)

END()
