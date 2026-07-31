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
    TStorageDevice Device;
    NProto::TAcquireDevicesRequest* Request;
    NProto::TAcquireDevicesResponse* Response;
};

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept
{
    NProto::TAcquireDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(std::move(params->Device.DeviceUUID));
    *params->Response = params->Device.Node->AcquireDevices(std::move(request));
    return 0;
}

struct TReleaseDevicesParams
{
    TStorageDevice Device;
    NProto::TReleaseDevicesRequest* Request;
    NProto::TReleaseDevicesResponse* Response;
};

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept
{
    NProto::TReleaseDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(std::move(params->Device.DeviceUUID));
    *params->Response = params->Device.Node->ReleaseDevices(std::move(request));
    return 0;
}

struct TWriteLogRecordParams
{
    TStorageDevice Device;
    NProto::TWriteLogRecordRequest* Request;
    NProto::TWriteLogRecordResponse* Response;
};

int WriteLogRecordFiberMain(TWriteLogRecordParams* params) noexcept
{
    NProto::TWriteLogRecordRequest request = *params->Request;
    request.SetDeviceUUID(std::move(params->Device.DeviceUUID));
    *params->Response = params->Device.Node->WriteLogRecord(std::move(request));
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupImpl: public IStorageGroup
{
private:
    TVector<TStorageDevice> Devices;
    std::atomic<ui32> Selector{0};

public:
    explicit TStorageGroupImpl(TVector<TStorageDevice> devices)
        : Devices(std::move(devices))
    {}

public:
    NProto::TError AcquireDevices() override
    {
        return MirrorRequest<
            NProto::TAcquireDevicesRequest,
            NProto::TAcquireDevicesResponse,
            TAcquireDevicesParams>(
            AcquireDevicesFiberMain,
            NProto::TAcquireDevicesRequest{});
    }

    NProto::TError ReleaseDevices() override
    {
        return MirrorRequest<
            NProto::TReleaseDevicesRequest,
            NProto::TReleaseDevicesResponse,
            TReleaseDevicesParams>(
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups) override
    {
        NProto::TWriteLogRecordRequest request;
        *request.MutableHeaders() = std::move(headers);
        request.MutablePageGroups()->Reserve(pageGroups.size());
        for (auto& pg: pageGroups) {
            auto* w = request.AddPageGroups();
            w->SetFirstPageNo(pg.FirstPageNo);
            w->MutableContent()->Reserve(pg.Content.size());
            for (auto& c: pg.Content) {
                *w->AddContent() = std::move(c);
            }
        }
        SILK_DEBUG("sg write: %s", DebugMessage(request).c_str());

        return MirrorRequest<
            NProto::TWriteLogRecordRequest,
            NProto::TWriteLogRecordResponse,
            TWriteLogRecordParams>(WriteLogRecordFiberMain, std::move(request));
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        pageGroups->clear();

        NProto::TReadPagesRequest request;
        const ui32 i =
            Selector.fetch_add(1, std::memory_order_relaxed) % Devices.size();
        request.SetDeviceUUID(Devices[i].DeviceUUID);
        *request.MutableHeaders() = std::move(headers);
        for (const auto& pg: pageGroupRefs) {
            auto* pgr = request.AddPageGroupRefs();
            pgr->SetPageSize(pg.PageSize);
            pgr->SetFirstPageNo(pg.FirstPageNo);
            pgr->SetPageCount(pg.PageCount);
        }
        SILK_DEBUG("sg read: %s", request.ShortUtf8DebugString().c_str());
        auto response = Devices[i].Node->ReadPages(request);
        if (!HasError(response.GetError())) {
            pageGroups->reserve(response.PageGroupsSize());
            for (auto& pg: *response.MutablePageGroups()) {
                auto& r = pageGroups->emplace_back();
                r.FirstPageNo = pg.GetFirstPageNo();
                r.Content.reserve(pg.ContentSize());
                for (auto& c: *pg.MutableContent()) {
                    r.Content.emplace_back(std::move(c));
                }
            }
        }

        return response.GetError();
    }

private:
    static TString DebugMessage(const NProto::TWriteLogRecordRequest& w)
    {
        NProto::TReadPagesRequest r;
        *r.MutableHeaders() = w.GetHeaders();
        for (const auto& pg: w.GetPageGroups()) {
            auto* rpg = r.AddPageGroupRefs();
            rpg->SetPageSize(pg.ContentSize() ? pg.GetContent(0).size() : 0);
            rpg->SetPageCount(pg.ContentSize());
            rpg->SetFirstPageNo(pg.GetFirstPageNo());
        }
        return r.ShortUtf8DebugString();
    }

    template <
        typename TRequest,
        typename TResponse,
        typename TParams,
        typename TFiberMain>
    NProto::TError MirrorRequest(TFiberMain fiberMain, TRequest request)
    {
        TVector<silk::FiberFuture> futures(Devices.size());
        TVector<TResponse> responses(Devices.size());
        for (ui32 i = 0; i < Devices.size(); ++i) {
            int r = silk::FiberScheduler::run(
                fiberMain,
                TParams{
                    .Device = Devices[i],
                    .Request = &request,
                    .Response = &responses[i]},
                &futures[i]);
            Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
        }

        NProto::TError error;
        for (ui32 i = 0; i < Devices.size(); ++i) {
            int r = futures[i].wait();
            if (r) {
                SILK_ERROR("future error: %s", ::strerror(r));
                if (!HasError(error)) {
                    error = MakeError(MAKE_SYSTEM_ERROR(r));
                }

                continue;
            }

            auto& nodeResponse = responses[i];
            if (HasError(nodeResponse.GetError())) {
                SILK_ERROR(
                    "node error: %s",
                    FormatError(nodeResponse.GetError()).c_str());
                if (!HasError(error)) {
                    error = std::move(*nodeResponse.MutableError());
                }
            }
        }

        return error;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<TStorageDevice> devices)
{
    return std::make_shared<TStorageGroupImpl>(std::move(devices));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
