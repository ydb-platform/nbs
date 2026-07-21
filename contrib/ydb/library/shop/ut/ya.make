UNITTEST()

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/shop
)

SRCS(
    estimator_ut.cpp
    flowctl_ut.cpp
    scheduler_ut.cpp
    lazy_scheduler_ut.cpp
)

END()
