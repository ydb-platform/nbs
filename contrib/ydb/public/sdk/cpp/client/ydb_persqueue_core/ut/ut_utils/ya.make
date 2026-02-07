LIBRARY()

SRCS(
    data_plane_helpers.h
    test_server.h
    ut_utils.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
)

END()
