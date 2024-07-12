GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
)

END()

RECURSE(
    cases
    cmd
    collate
    currency
    date
    encoding
    feature
    internal
    language
    message
    number
    runes
    search
    secure
    transform
    unicode
    width
)
