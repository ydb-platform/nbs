PY23_LIBRARY()

PY_SRCS(
    control_plane.py
    data_plane.py
    test_yds_base.py
)

PEERDIR(
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
)

END()
