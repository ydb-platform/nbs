UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    main.cpp
    self_heal_actor_ut.cpp
    defs.h
    env.h
    events.h
    node_warden_mock.h
    timer_actor.h
    vdisk_mock.h
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/core/blobstorage/dsproxy/mock
    contrib/ydb/core/blobstorage/pdisk/mock
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
