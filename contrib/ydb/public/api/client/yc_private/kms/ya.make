PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

GRPC()
SRCS(
    symmetric_crypto_service.proto
    symmetric_key.proto
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/kms/asymmetricencryption
    contrib/ydb/public/api/client/yc_private/kms/asymmetricsignature
)

END()

