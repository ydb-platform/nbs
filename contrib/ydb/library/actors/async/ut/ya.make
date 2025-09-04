UNITTEST_FOR(contrib/ydb/library/actors/async)

PEERDIR(
    contrib/ydb/library/actors/testlib
)

SRCS(
    async_ut.cpp
    callback_coroutine_ut.cpp
    cancellation_ut.cpp
    continuation_ut.cpp
    event_ut.cpp
    sleep_ut.cpp
    task_group_ut.cpp
    timeout_ut.cpp
    wait_for_event_ut.cpp
)

END()
