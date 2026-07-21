LIBRARY()

SRCS(
    yql_yt_fmr_indexed_block_reader.cpp
    yql_yt_fmr_sorting_block_reader.cpp
    yql_yt_job_impl.cpp
    yql_yt_raw_table_queue_reader.cpp
    yql_yt_raw_table_queue_writer.cpp
    yql_yt_reduce_reader.cpp
    yql_yt_sorted_merge_reader.cpp
    yql_yt_table_data_service_reader.cpp
    yql_yt_table_data_service_base_writer.cpp
    yql_yt_table_data_service_writer.cpp
    yql_yt_table_data_service_sorted_writer.cpp
    yql_yt_table_index_marker.cpp
    yql_yt_table_queue_writer_with_lock.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/job/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
    contrib/ydb/library/yql/providers/yt/fmr/utils/hasher
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_launcher
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/interface
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/tvm/impl
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/impl
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
