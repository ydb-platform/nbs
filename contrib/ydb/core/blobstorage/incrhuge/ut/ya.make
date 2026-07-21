UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/protos
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blobstorage/incrhuge
    contrib/ydb/core/blobstorage/pdisk
)

SRCS(
    incrhuge_basic_ut.cpp
    incrhuge_id_dict_ut.cpp
    incrhuge_log_merger_ut.cpp
)

END()
