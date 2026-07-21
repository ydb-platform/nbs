UNITTEST_FOR(contrib/ydb/core/persqueue)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SPLIT_FACTOR(400)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/fmt
    library/cpp/getopt
    library/cpp/json
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/persqueue/writer
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/sdk/cpp/src/library/kafka
)

YQL_LAST_ABI_VERSION()

SRCS(
    counters_ut.cpp
    pqtablet_mock.cpp
    internals_ut.cpp
    inflight_limiter_ut.cpp
    make_config.cpp
    metering_sink_ut.cpp
    partition_chooser_ut.cpp
    partitioning_keys_manager_ut.cpp
    pq_ut.cpp
    partition_ut.cpp
    partitiongraph_ut.cpp
    pqtablet_ut.cpp
    sourceid_ut.cpp
    user_info_ut.cpp
    pqrb_describes_ut.cpp
    partition_scale_manager_graph_cmp_ut.cpp
    utils_ut.cpp
    events_ut.cpp
    write_id_ut.cpp
    pqdata_transaction_compat_ut.cpp
)

RESOURCE(
    contrib/ydb/core/persqueue/ut/resources/counters_datastreams.html counters_datastreams.html
    contrib/ydb/core/persqueue/ut/resources/counters_pqproxy_firstclass.html counters_pqproxy_firstclass.html
    contrib/ydb/core/persqueue/ut/resources/counters_topics.html counters_topics.html
    contrib/ydb/core/persqueue/ut/resources/counters_topics_extended.html counters_topics_extended.html

    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation/after_write.html federation_after_write.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation/after_read.html federation_after_read.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation/turned_off.html federation_turned_off.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/after_write.html first_class_citizen_after_write.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/after_read.html first_class_citizen_after_read.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/turned_off.html first_class_citizen_turned_off.html

    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation_with_monitoring_project_id/after_write.html federation_with_monitoring_project_id_after_write.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation_with_monitoring_project_id/after_read.html federation_with_monitoring_project_id_after_read.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/federation_with_monitoring_project_id/turned_off.html federation_with_monitoring_project_id_turned_off.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen_with_monitoring_project_id/after_write.html first_class_citizen_with_monitoring_project_id_after_write.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen_with_monitoring_project_id/after_read.html first_class_citizen_with_monitoring_project_id_after_read.html
    contrib/ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen_with_monitoring_project_id/turned_off.html first_class_citizen_with_monitoring_project_id_turned_off.html

    contrib/ydb/core/persqueue/ut/resources/counters_pqproxy.html counters_pqproxy.html

    contrib/ydb/core/persqueue/ut/resources/counters_labeled.json counters_labeled.json
)

END()
