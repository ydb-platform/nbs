#pragma once

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/service.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##ServiceMethod                                              \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#name);                 \
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        using TRequestEvent = TEvService::TEv##name##Request;                  \
        using TResponseEvent = TEvService::TEv##name##Response;                \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_REMOTE_SERVICE(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##ServiceMethod                                              \
    {                                                                          \
        static constexpr auto RequestName = TStringBuf(#name);                 \
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_LOCAL_DATA_METHODS(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

}   // namespace NCloud::NFileStore::NStorage
