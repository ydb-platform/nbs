UNITTEST_FOR(contrib/ydb/core/persqueue)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SPLIT_FACTOR(40)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    blob_ut.cpp
    counters_ut.cpp
    pqtablet_mock.cpp
    internals_ut.cpp
    make_config.cpp
    metering_sink_ut.cpp
    partition_chooser_ut.cpp
    pq_ut.cpp
    partition_ut.cpp
    partitiongraph_ut.cpp
    pqtablet_ut.cpp
    quota_tracker_ut.cpp
    sourceid_ut.cpp
    type_codecs_ut.cpp
    user_info_ut.cpp
    pqrb_describes_ut.cpp
    microseconds_sliding_window_ut.cpp
    fetch_request_ut.cpp
    utils_ut.cpp
    list_all_topics_ut.cpp
    cache_eviction_ut.cpp
)

RESOURCE(
    contrib/ydb/core/persqueue/ut/resources/counters_datastreams.html counters_datastreams.html
    contrib/ydb/core/persqueue/ut/resources/counters_pqproxy_firstclass.html counters_pqproxy_firstclass.html
    contrib/ydb/core/persqueue/ut/resources/counters_topics.html counters_topics.html

    contrib/ydb/core/persqueue/ut/resources/counters_pqproxy.html counters_pqproxy.html

    contrib/ydb/core/persqueue/ut/resources/counters_labeled.json counters_labeled.json
)

END()
