GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    file.go
    if.go
    lock.go
    prop.go
    webdav.go
    xml.go
)

END()

RECURSE(
    internal
)
