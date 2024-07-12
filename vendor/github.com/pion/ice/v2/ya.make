GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SIZE(MEDIUM)

# Some tests require IPv4

GO_SKIP_TESTS(
    TestActiveTCP
    TestListenUDP
    TestMulticastDNSMixedConnection
    TestMulticastDNSOnlyConnection
    TestMulticastDNSStaticHostName
    TestMuxAgent
    TestUnspecifiedUDPMux
)

SRCS(
    active_tcp.go
    addr.go
    agent.go
    agent_config.go
    agent_handlers.go
    agent_stats.go
    candidate.go
    candidate_base.go
    candidate_host.go
    candidate_peer_reflexive.go
    candidate_relay.go
    candidate_server_reflexive.go
    candidatepair.go
    candidatepair_state.go
    candidaterelatedaddress.go
    candidatetype.go
    context.go
    errors.go
    external_ip_mapper.go
    gather.go
    ice.go
    icecontrol.go
    mdns.go
    net.go
    networktype.go
    priority.go
    rand.go
    role.go
    selection.go
    stats.go
    tcp_mux.go
    tcp_mux_multi.go
    tcp_packet_conn.go
    tcptype.go
    test_utils.go
    transport.go
    udp_mux.go
    udp_mux_multi.go
    udp_mux_universal.go
    udp_muxed_conn.go
    url.go
    usecandidate.go
)

GO_TEST_SRCS(
    active_tcp_test.go
    agent_get_best_available_candidate_pair_test.go
    agent_get_best_valid_candidate_pair_test.go
    agent_on_selected_candidate_pair_change_test.go
    agent_test.go
    agent_udpmux_test.go
    candidate_relay_test.go
    candidate_server_reflexive_test.go
    candidate_test.go
    candidatepair_test.go
    connectivity_vnet_test.go
    external_ip_mapper_test.go
    gather_test.go
    gather_vnet_test.go
    ice_test.go
    icecontrol_test.go
    mdns_test.go
    net_test.go
    networktype_test.go
    priority_test.go
    rand_test.go
    tcp_mux_multi_test.go
    tcp_mux_test.go
    tcptype_test.go
    transport_test.go
    transport_vnet_test.go
    udp_mux_multi_test.go
    udp_mux_test.go
    udp_mux_universal_test.go
    usecandidate_test.go
)

END()

RECURSE(
    gotest
    internal
)
