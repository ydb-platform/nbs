GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    auth.go
    auth_token.go
    client.go
    help.go
    kv.go
    kv_v1.go
    kv_v2.go
    lifetime_watcher.go
    logical.go
    output_policy.go
    output_string.go
    plugin_helpers.go
    plugin_runtime_types.go
    plugin_types.go
    replication_status.go
    request.go
    response.go
    secret.go
    ssh.go
    ssh_agent.go
    sudo_paths.go
    sys.go
    sys_audit.go
    sys_auth.go
    sys_capabilities.go
    sys_config_cors.go
    sys_generate_root.go
    sys_hastatus.go
    sys_health.go
    sys_init.go
    sys_leader.go
    sys_leases.go
    sys_mfa.go
    sys_monitor.go
    sys_mounts.go
    sys_plugins.go
    sys_plugins_runtimes.go
    sys_policy.go
    sys_raft.go
    sys_rekey.go
    sys_rotate.go
    sys_seal.go
    sys_stepdown.go
    sys_ui_custom_message.go
)

GO_TEST_SRCS(
    api_test.go
    auth_test.go
    client_test.go
    kv_test.go
    output_policy_test.go
    plugin_types_test.go
    renewer_test.go
    request_test.go
    secret_test.go
    ssh_agent_test.go
    sudo_paths_test.go
    sys_mounts_test.go
    sys_plugins_runtimes_test.go
    sys_plugins_test.go
    sys_ui_custom_message_test.go
)

END()

RECURSE(
    # gotest
)
