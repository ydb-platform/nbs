LIBRARY()

SRCS(
    actor_pool.cpp
    helpers.cpp
    mortal_actor.cpp
    poison_pill_helper.cpp
    pooled_actor.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
