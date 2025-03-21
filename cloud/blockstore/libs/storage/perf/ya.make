Y_BENCHMARK(cloud-blockstore-libs-storage-perf)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

BENCHMARK_OPTS(--budget=10)

PEERDIR(
    cloud/blockstore/libs/storage/partition2/model

    library/cpp/archive
    library/cpp/json
    library/cpp/string_utils/base64
)

SRCS(
    blob_index_perf.cpp
    block_list_perf.cpp
)

ARCHIVE(
    NAME data.inc

    data/blocklists.json
)

END()
