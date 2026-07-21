PY3_PROGRAM(iam_grpc_emulator)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    contrib/python/grpcio
    contrib/ydb/public/api/client/yc_private/iam
)

END()
