GO_LIBRARY()

SRCS(
    convert.go
    ctxutil.go
    sql.go
)

END()

RECURSE(
    database
    driver
)
