GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

BUILD_ONLY_IF(OS_LINUX)

SRCS(
    chain.go
    compat_policy.go
    conn.go
    counter.go
    doc.go
    flowtable.go
    monitor.go
    obj.go
    quota.go
    rule.go
    set.go
    table.go
    util.go
)

GO_TEST_SRCS(
    compat_policy_test.go
    set_test.go
    util_test.go
)

GO_XTEST_SRCS(
    # monitor_test.go
    # nftables_test.go
)

END()

RECURSE(
    alignedbuff
    binaryutil
    expr
    gotest
    internal
    userdata
    xt
)
