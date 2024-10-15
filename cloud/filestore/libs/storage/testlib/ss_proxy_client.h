#pragma once

#include "test_env.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyClient
{
private:
    const TStorageConfigPtr StorageConfig;

    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;

public:
    TSSProxyClient(TStorageConfigPtr config, NKikimr::TTestActorRuntime& runtime, ui32 nodeIdx)
        : StorageConfig(std::move(config))
        , Runtime(runtime)
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

    auto CreateDescribeSchemeRequest(const TString& path)
    {
        return std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeRequest>(path);
    }

    auto CreateModifySchemeRequest(const NKikimrSchemeOp::TModifyScheme& modifyScheme)
    {
        return std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(modifyScheme);
    }

    auto CreateDescribeFileStoreRequest(const TString& path)
    {
        return std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(path);
    }

    auto CreateCreateFileStoreRequest(
        const TString& fileSystemId,
        ui64 blocksCount,
        ui32 blockSize = DefaultBlockSize)
    {
        NKikimrFileStore::TConfig config;
        config.SetFileSystemId(fileSystemId);
        config.SetCloudId("test");
        config.SetFolderId("test");
        config.SetBlockSize(blockSize);
        config.SetBlocksCount(blocksCount);

        SetupFileStorePerformanceAndChannels(
            false,  // do not allocate mixed0 channel
            *StorageConfig,
            config,
            {}      // clientPerformanceProfile
        );

        return std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(config);
    }

    auto CreateDestroyFileStoreRequest(const TString& fileSystemId)
    {
        return std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(fileSystemId);
    }

#define FILESTORE_DECLARE_METHOD(name, ns)                                     \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeSSProxyServiceId(), std::move(request));               \
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
        SendRequest(MakeSSProxyServiceId(), std::move(request));               \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// FILESTORE_DECLARE_METHOD

    FILESTORE_DECLARE_METHOD(DescribeScheme, TEvStorageSSProxy)
    FILESTORE_DECLARE_METHOD(ModifyScheme, TEvStorageSSProxy)
    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_DECLARE_METHOD, TEvSSProxy)

#undef FILESTORE_DECLARE_METHOD
};

}   // namespace NCloud::NFileStore::NStorage
