#pragma once

#include "test_env.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TIndexTabletProxyClient
{
private:
    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;

public:
    TIndexTabletProxyClient(NKikimr::TTestActorRuntime& runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {}

    const NActors::TActorId& GetSender() const
    {
        return Sender;
    }

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release(),
            0,          // flags
            0,          // cookie
            // forwardOnNondelivery
            nullptr);

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    auto RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateWaitReadyRequest(const TString& fileSystemId)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvWaitReadyRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        return request;
    }

    auto CreateListNodesRequest(const TString& fileSystemId, ui64 nodeId)
    {
        auto request = std::make_unique<TEvService::TEvListNodesRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        return request;
    }

#define FILESTORE_DECLARE_METHOD(name, ns)                                     \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeIndexTabletProxyServiceId(), std::move(request));      \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeIndexTabletProxyServiceId(), std::move(request));      \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// FILESTORE_DECLARE_METHOD

    FILESTORE_SERVICE_REQUESTS(FILESTORE_DECLARE_METHOD, TEvService)
    FILESTORE_TABLET_REQUESTS(FILESTORE_DECLARE_METHOD, TEvIndexTablet)

#undef FILESTORE_DECLARE_METHOD
};

}   // namespace NCloud::NFileStore::NStorage
