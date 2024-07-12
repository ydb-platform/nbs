GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    apply.go
    cmd.go
    destroy.go
    doc.go
    errors.go
    fmt.go
    force_unlock.go
    get.go
    graph.go
    import.go
    init.go
    metadata_functions.go
    options.go
    output.go
    plan.go
    providers_lock.go
    providers_schema.go
    refresh.go
    show.go
    state_mv.go
    state_pull.go
    state_push.go
    state_rm.go
    taint.go
    terraform.go
    untaint.go
    upgrade012.go
    upgrade013.go
    validate.go
    version.go
    workspace_delete.go
    workspace_list.go
    workspace_new.go
    workspace_select.go
    workspace_show.go
)

GO_TEST_SRCS(
    # apply_test.go
    # cmd_test.go
    # destroy_test.go
    # fmt_test.go
    # force_unlock_test.go
    # get_test.go
    # graph_test.go
    # import_test.go
    # init_test.go
    # metadata_functions_test.go
    # output_test.go
    # plan_test.go
    # providers_lock_test.go
    # providers_schema_test.go
    # refresh_test.go
    # show_test.go
    # state_mv_test.go
    # state_pull_test.go
    # state_push_test.go
    # state_rm_test.go
    # taint_test.go
    # terraform_test.go
    # untaint_test.go
    # upgrade012_test.go
    # upgrade013_test.go
    # version_test.go
    # workspace_delete_test.go
    # workspace_list_test.go
    # workspace_new_test.go
    # workspace_show_test.go
)

IF (OS_LINUX)
    SRCS(
        cmd_linux.go
    )

    GO_TEST_SRCS(
        # cmd_linux_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        cmd_default.go
    )

    GO_TEST_SRCS(
        # cmd_default_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        cmd_default.go
    )

    GO_TEST_SRCS(
        # cmd_default_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
    internal
)
