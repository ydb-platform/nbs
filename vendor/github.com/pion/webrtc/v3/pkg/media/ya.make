GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    media.go
)

GO_XTEST_SRCS(media_test.go)

END()

RECURSE(
    gotest
    h264reader
    h264writer
    ivfreader
    ivfwriter
    oggreader
    oggwriter
    rtpdump
    samplebuilder
)
