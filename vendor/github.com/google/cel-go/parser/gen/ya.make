GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    cel_base_listener.go
    cel_base_visitor.go
    cel_lexer.go
    cel_listener.go
    cel_parser.go
    cel_visitor.go
    doc.go
)

END()
