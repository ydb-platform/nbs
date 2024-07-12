GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    copy.go
    generate.go
    ssh.go
    ssh.pb.go
)

END()

RECURSE(
    sshprovider
)
