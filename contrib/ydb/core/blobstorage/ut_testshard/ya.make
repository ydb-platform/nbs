UNITTEST()

    SIZE(MEDIUM)
    TIMEOUT(600)

    REQUIREMENTS(
        ram:32
    )

    SRCS(
        main.cpp
    )

    PEERDIR(
        contrib/ydb/apps/version
        contrib/ydb/core/base
        contrib/ydb/core/blob_depot
        contrib/ydb/core/blobstorage/backpressure
        contrib/ydb/core/blobstorage/dsproxy/mock
        contrib/ydb/core/blobstorage/nodewarden
        contrib/ydb/core/blobstorage/pdisk/mock
        contrib/ydb/core/blobstorage/testing/group_overseer
        contrib/ydb/core/blobstorage/vdisk/common
        contrib/ydb/core/mind
        contrib/ydb/core/mind/bscontroller
        contrib/ydb/core/mind/hive
        contrib/ydb/core/sys_view/service
        contrib/ydb/core/test_tablet
        contrib/ydb/core/tx/scheme_board
        contrib/ydb/core/tx/tx_allocator
        contrib/ydb/core/tx/mediator
        contrib/ydb/core/tx/coordinator
        contrib/ydb/core/tx/scheme_board
        contrib/ydb/core/util
        contrib/ydb/library/yql/public/udf/service/stub
        contrib/ydb/library/yql/sql/pg_dummy
        library/cpp/testing/unittest
    )

END()
