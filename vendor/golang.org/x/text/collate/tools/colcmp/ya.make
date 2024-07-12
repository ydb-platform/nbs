GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    chars.go
    col.go
    colcmp.go
    gen.go
)

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(darwin.go)
ENDIF()

END()
