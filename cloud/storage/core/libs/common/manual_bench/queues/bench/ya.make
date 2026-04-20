Y_BENCHMARK(common-queues-bench)
SIZE(MEDIUM)
TIMEOUT(600)

TAG(ya:manual)

SRCS(
    ../main.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()
