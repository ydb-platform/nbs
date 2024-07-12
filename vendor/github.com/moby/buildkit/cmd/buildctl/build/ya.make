GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    allow.go
    build.go
    exportcache.go
    importcache.go
    local.go
    ocilayout.go
    opt.go
    output.go
    secret.go
    ssh.go
    util.go
)

GO_TEST_SRCS(
    exportcache_test.go
    importcache_test.go
)

END()

RECURSE(
    gotest
)
