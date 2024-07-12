GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    common.go
    doc.go
)

END()

RECURSE(
    kit
    logrus
    settable
    zap
)
