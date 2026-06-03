LIBRARY()

SRCS(
    auth_provider_kikimr.cpp
    handler_actor.cpp
    methods.cpp
    request_actor.cpp
    service.cpp
    side_channel.cpp
    stream_request_actor.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/api
    cloud/storage/core/libs/auth
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/kikimr
)

END()

RECURSE_FOR_TESTS(ut)
