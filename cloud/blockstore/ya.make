RECURSE(
    apps
    config
    libs
    private
    public
    pylibs
    tools
    vhost-server
)

RECURSE_FOR_TESTS(
    tests
)

CFLAGS(
    -DNETLINK
)
