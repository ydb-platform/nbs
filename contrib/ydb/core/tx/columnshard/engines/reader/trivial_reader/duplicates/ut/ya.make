UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/blobs_action/counters
    contrib/ydb/core/tx/columnshard/column_fetching
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/data_accessor
    contrib/ydb/core/tx/columnshard/data_accessor/abstract
    contrib/ydb/core/tx/columnshard/data_accessor/local_db
    contrib/ydb/core/tx/columnshard/data_sharing/manager
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/tx/columnshard/engines/reader/common
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/common
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor
    contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates
    contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/engines/storage/chunks
    contrib/ydb/core/tx/columnshard/engines/storage/indexes
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/conveyor_composite/usage
    contrib/ydb/core/tx/general_cache/usage
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_filters.cpp
    ut_manager.cpp
)

END()
