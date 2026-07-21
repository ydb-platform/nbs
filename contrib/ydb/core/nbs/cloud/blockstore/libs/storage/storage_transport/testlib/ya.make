LIBRARY()

SRCS(
    ddisk_stub_actor.cpp
    ic_storage_transport_test_adapter.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    contrib/ydb/core/testlib
    contrib/ydb/library/actors/core
)

END()
