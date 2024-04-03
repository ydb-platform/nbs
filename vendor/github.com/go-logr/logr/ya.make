GO_LIBRARY()

LICENSE(Apache-2.0)

IF(GOSTD_VERSION == 1.21)
    SRCS(context_slog.go)
ELSE()
    SRCS(context_noslog.go)
ENDIF()

SRCS(
    context.go
    discard.go
    logr.go
    sloghandler.go
    slogr.go
    slogsink.go
)

END()
