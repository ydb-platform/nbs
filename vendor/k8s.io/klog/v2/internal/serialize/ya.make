GO_LIBRARY()

LICENSE(Apache-2.0)

IF(GOSTD_VERSION == 1.21)
    SRCS(keyvalues_slog.go)
ELSE()
    SRCS(keyvalues_no_slog.go)
ENDIF()

SRCS(
    keyvalues.go
)

END()
