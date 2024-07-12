GO_LIBRARY(pointer)

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
)

IF (CGO_ENABLED)
    CGO_SRCS(pointer.go)
ENDIF()

END()
