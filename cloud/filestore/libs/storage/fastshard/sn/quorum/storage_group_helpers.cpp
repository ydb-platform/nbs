#include "storage_group_helpers.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

NProto::TWriteLogRecordRequest MakeWriteLogRecordRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroup>& pageGroups,
    ui64 lsn)
{
    NProto::TWriteLogRecordRequest request;
    headers.SetClientId(DefaultClientId);
    *request.MutableHeaders() = std::move(headers);
    request.MutablePageGroups()->Reserve(pageGroups.size());
    for (const auto& pg: pageGroups) {
        auto* w = request.AddPageGroups();
        w->SetFirstPageNo(pg.FirstPageNo);
        w->MutableContent()->Reserve(pg.Content.size());
        for (const auto& c: pg.Content) {
            // proto content is TString - the copy stays until the protocol
            // itself switches away from TString
            w->AddContent()->assign(c.Data(), c.Size());
        }
    }
    request.SetLogSequenceNumber(lsn);
    return request;
}

NProto::TReadPagesRequest MakeReadPagesRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroupRef>& pageGroupRefs)
{
    NProto::TReadPagesRequest request;
    headers.SetClientId(DefaultClientId);
    *request.MutableHeaders() = std::move(headers);
    for (const auto& pg: pageGroupRefs) {
        auto* ref = request.AddPageGroupRefs();
        ref->SetPageSize(pg.PageSize);
        ref->SetFirstPageNo(pg.FirstPageNo);
        ref->SetPageCount(pg.PageCount);
    }
    return request;
}

void ExtractPageGroups(
    const NProto::TReadPagesResponse& response,
    TVector<TPageGroup>* pageGroups)
{
    pageGroups->reserve(response.PageGroupsSize());
    for (const auto& pg: response.GetPageGroups()) {
        auto& r = pageGroups->emplace_back();
        r.FirstPageNo = pg.GetFirstPageNo();
        r.Content.reserve(pg.ContentSize());
        for (const auto& c: pg.GetContent()) {
            r.Content.emplace_back(c.data(), c.size());
        }
    }
}

TString DebugMessage(const NProto::TWriteLogRecordRequest& w)
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

////////////////////////////////////////////////////////////////////////////////

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept
{
    NProto::TAcquireDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(params->Device.DeviceUUID);
    *params->Response = CallWithRetries(
        *params->RetryPolicy,
        *params->Timer,
        [&] { return params->Device.Node->AcquireDevices(request); });
    return 0;
}

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept
{
    NProto::TReleaseDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(params->Device.DeviceUUID);
    *params->Response = CallWithRetries(
        *params->RetryPolicy,
        *params->Timer,
        [&] { return params->Device.Node->ReleaseDevices(request); });
    return 0;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
