LIBRARY()

SRCS(
    helpers.cpp
    poison_pill_helper.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
