G_BENCHMARK(benchmark_actors_async)

SRCS(
    b_actor_async.cpp
)

PEERDIR(
    contrib/ydb/library/actors/async
    contrib/ydb/library/actors/core
)

END()
