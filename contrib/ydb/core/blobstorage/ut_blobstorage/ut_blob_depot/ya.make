UNITTEST_FOR(contrib/ydb/core/blobstorage/ut_blobstorage)

    SRCS(
        blob_depot.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        contrib/ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
