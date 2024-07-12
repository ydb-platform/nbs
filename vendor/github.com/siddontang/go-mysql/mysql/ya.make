GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    const.go
    errcode.go
    errname.go
    error.go
    field.go
    gtid.go
    mariadb_gtid.go
    mysql_gtid.go
    parse_binary.go
    position.go
    result.go
    resultset.go
    resultset_helper.go
    state.go
    util.go
)

GO_TEST_SRCS(
    mariadb_gtid_test.go
    mysql_test.go
)

END()

RECURSE(
    gotest
)
