UNITTEST()

    SIZE(MEDIUM)

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
        yql/essentials/public/udf/service/stub
        yql/essentials/sql/pg_dummy
        library/cpp/testing/unittest
        contrib/ydb/core/util/actorsys_test
    )

END()
