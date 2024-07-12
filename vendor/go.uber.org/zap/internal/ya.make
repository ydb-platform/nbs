GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    level_enabler.go
)

END()

RECURSE(
    bufferpool
    color
    exit
    pool
    readme
    stacktrace
    ztest
)
