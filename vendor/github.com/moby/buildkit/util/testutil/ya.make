GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    imageinfo.go
    tar.go
)

END()

RECURSE(
    containerd
    dockerd
    echoserver
    httpserver
    integration
)
