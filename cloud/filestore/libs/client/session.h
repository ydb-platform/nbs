#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SESSION_FORWARD(xxx, ...) \
    xxx(GetFileStoreInfo, __VA_ARGS__)      \
                                            \
    xxx(AddClusterNode, __VA_ARGS__)        \
    xxx(RemoveClusterNode, __VA_ARGS__)     \
    xxx(ListClusterNodes, __VA_ARGS__)      \
    xxx(AddClusterClients, __VA_ARGS__)     \
    xxx(RemoveClusterClients, __VA_ARGS__)  \
    xxx(ListClusterClients, __VA_ARGS__)    \
    xxx(UpdateCluster, __VA_ARGS__)         \
    // FILESTORE_SESSION_FORWARD

////////////////////////////////////////////////////////////////////////////////

struct ISession: public IFileStore
{
    virtual ~ISession() = default;

    virtual NThreading::TFuture<NProto::TCreateSessionResponse> CreateSession(
        bool readOnly = false,
        ui64 mountSeqNumber = 0) = 0;
    virtual NThreading::TFuture<NProto::TCreateSessionResponse> AlterSession(
        bool readOnly,
        ui64 mountSeqNumber) = 0;
    virtual NThreading::TFuture<NProto::TDestroySessionResponse>
    DestroySession() = 0;

#define FILESTORE_DECLARE_METHOD(name, ...)                      \
    virtual NThreading::TFuture<NProto::T##name##Response> name( \
        TCallContextPtr callContext,                             \
        std::shared_ptr<NProto::T##name##Request> request) = 0;  \
    // FILESTORE_DECLARE_METHOD

    FILESTORE_SESSION_FORWARD(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IFileStoreServicePtr client,
    TSessionConfigPtr config);

}   // namespace NCloud::NFileStore::NClient
