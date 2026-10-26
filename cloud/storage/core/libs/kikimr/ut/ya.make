UNITTEST_FOR(cloud/storage/core/libs/kikimr)

SRCS(
    config_dispatcher_helpers_ut.cpp
    config_initializer_ut.cpp
    node_registration_helpers_ut.cpp
    node_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics/bootstrap_monitoring
    library/cpp/http/simple
    library/cpp/testing/gmock_in_unittest
)

END()
