#include "storage_group.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAcquireDevicesParams
{
    IStorageNodePtr Node;
    NProto::TAcquireDevicesRequest* Request;
    NProto::TAcquireDevicesResponse* Response;
};

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept
{
    *params->Response = params->Node->AcquireDevices(*params->Request);
    return 0;
}

struct TReleaseDevicesParams
{
    IStorageNodePtr Node;
    NProto::TReleaseDevicesRequest* Request;
    NProto::TReleaseDevicesResponse* Response;
};

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept
{
    *params->Response = params->Node->ReleaseDevices(*params->Request);
    return 0;
}

struct TWriteLogRecordParams
{
    IStorageNodePtr Node;
    NProto::TWriteLogRecordRequest* Request;
    NProto::TWriteLogRecordResponse* Response;
};

int WriteLogRecordFiberMain(TWriteLogRecordParams* params) noexcept
{
    *params->Response = params->Node->WriteLogRecord(*params->Request);
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupImpl: public IStorageGroup
{
private:
    TVector<IStorageNodePtr> Nodes;
    std::atomic<ui32> Selector{0};

public:
    explicit TStorageGroupImpl(TVector<IStorageNodePtr> nodes)
        : Nodes(std::move(nodes))
    {}

public:
    NProto::TAcquireDevicesResponse AcquireDevices(
        NProto::TAcquireDevicesRequest request) override
    {
        return MirrorRequest<
            NProto::TAcquireDevicesRequest,
            NProto::TAcquireDevicesResponse,
            TAcquireDevicesParams
        >(AcquireDevicesFiberMain, std::move(request));
    }

    NProto::TReleaseDevicesResponse ReleaseDevices(
        NProto::TReleaseDevicesRequest request) override
    {
        return MirrorRequest<
            NProto::TReleaseDevicesRequest,
            NProto::TReleaseDevicesResponse,
            TReleaseDevicesParams
        >(ReleaseDevicesFiberMain, std::move(request));
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        return MirrorRequest<
            NProto::TWriteLogRecordRequest,
            NProto::TWriteLogRecordResponse,
            TWriteLogRecordParams
        >(WriteLogRecordFiberMain, std::move(request));
    }

    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        const ui32 i =
            Selector.fetch_add(1, std::memory_order_relaxed) % Nodes.size();
        // TODO: update the request with the right device uuid
        return Nodes[i]->ReadPages(request);
    }

private:
    template <
        typename TRequest,
        typename TResponse,
        typename TParams,
        typename TFiberMain>
    TResponse MirrorRequest(TFiberMain fiberMain, TRequest request)
    {
        TVector<silk::FiberFuture> futures(Nodes.size());
        TVector<TResponse> responses(Nodes.size());
        for (ui32 i = 0; i < Nodes.size(); ++i) {
            int r = silk::FiberScheduler::run(
                fiberMain,
                TParams{
                    .Node = Nodes[i],
                    .Request = &request,
                    .Response = &responses[i]},
                &futures[i]);
            Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
        }

        TResponse response;
        for (ui32 i = 0; i < Nodes.size(); ++i) {
            int r = futures[i].wait();
            if (r) {
                SILK_ERROR("future error: %s", ::strerror(r));
                if (!HasError(response.GetError())) {
                    *response.MutableError() = MakeError(MAKE_SYSTEM_ERROR(r));
                }

                continue;
            }

            auto& nodeResponse = responses[i];
            if (HasError(nodeResponse.GetError())) {
                SILK_ERROR(
                    "node error: %s",
                    FormatError(nodeResponse.GetError()).c_str());
                if (!HasError(response.GetError())) {
                    *response.MutableError() =
                        std::move(*nodeResponse.MutableError());
                }
            }
        }

        return response;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<IStorageNodePtr> nodes)
{
    return std::make_shared<TStorageGroupImpl>(std::move(nodes));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
