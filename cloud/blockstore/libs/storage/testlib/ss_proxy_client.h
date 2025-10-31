#pragma once

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyClient
{
private:
    NActors::TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    NActors::TActorId Sender;

public:
    explicit TSSProxyClient(NActors::TTestActorRuntime& runtime, ui32 nodeIdx = 0)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(NodeIdx))
    {}

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release());

        Runtime.Send(ev);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, TDuration::Seconds(5));

        UNIT_ASSERT(handle);
        std::unique_ptr<TResponse> response(handle->Release<TResponse>().Release());

        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());

        return response;
    }

    void CreateVolume(const TString& diskId, NKikimrBlockStore::TVolumeConfig config = {})
    {
        config.SetDiskId(diskId);

        if (config.GetStorageMediaKind() == NProto::STORAGE_MEDIA_DEFAULT) {
            config.SetStorageMediaKind(diskId.StartsWith("nonrepl-")
                ? NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
                : diskId.StartsWith("hdd-nonrepl-")
                ? NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED
                : diskId.StartsWith("mirror2-")
                ? NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2
                : diskId.StartsWith("mirror3-")
                ? NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3
                : diskId.StartsWith("ssd-local-")
                ? NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL
                : diskId.StartsWith("hdd-local-")
                ? NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL
                : NCloud::NProto::STORAGE_MEDIA_SSD);
        }

        auto request = std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
            std::move(config));

        SendRequest(MakeSSProxyServiceId(), std::move(request));
        RecvResponse<TEvSSProxy::TEvCreateVolumeResponse>();
    }
};

}   // namespace NCloud::NBlockStore::NStorage
