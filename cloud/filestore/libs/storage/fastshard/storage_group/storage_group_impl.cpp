#include "storage_group_impl.h"

#include "storage_group_helpers.h"

#include <cloud/fastshard/libs/fiber/event.h>

#include <silk/fibers/fiber.h>
#include <silk/util/logger.h>

#include <util/digest/city.h>
#include <util/stream/format.h>
#include <util/string/builder.h>

#include <algorithm>
#include <cstring>
#include <mutex>

////////////////////////////////////////////////////////////////////////////////

using NCloud::NFileStore::NStorage::NFastShard::TStorageGroupHeader;

template <>
void Out<TStorageGroupHeader>(
    IOutputStream& out,
    const TStorageGroupHeader& header)
{
    out << "{Magic: " << Hex(header.MagicNumber)
        << ", Version: " << static_cast<ui32>(header.Version)
        << ", GroupType: " << static_cast<ui32>(header.GroupType)
        << ", PageSize: " << header.PageSize
        << ", DeviceUUIDHash: " << Hex(header.DeviceUUIDHash) << "}";
}

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::TFastShardContext;
using NCloud::NFastShard::WaitFor;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ReservedPages = TStorageGroupHeader::StorageGroupReservedPages;

// TODO(#6957): unify with blockstore
bool IsAllZeroes(const char* src, size_t size)
{
    if (size < sizeof(ui64)) {
        return std::all_of(src, src + size, [](char c) { return c == 0; });
    }

    const bool aligned = reinterpret_cast<uintptr_t>(src) % sizeof(ui64) == 0;
    const size_t step = aligned ? sizeof(ui64) : 1;
    return std::all_of(src, src + step, [](char c) { return c == 0; })
        && memcmp(src, src + step, size - step) == 0;
}

ui64 HashDeviceUUID(TStringBuf deviceUUID)
{
    return CityHash64(deviceUUID.data(), deviceUUID.size());
}

TString MakeHeaderPage(const TStorageGroupHeader& header, ui32 pageSize)
{
    TString page(pageSize, '\0');
    std::memcpy(page.begin(), &header, sizeof(header));
    return page;
}

NProto::TError CheckPageRange(ui64 firstPageNo, ui64 pageCount)
{
    // firstPageNo + ReservedPages + pageCount must not wrap around.
    if (firstPageNo > Max<ui64>() - ReservedPages ||
        pageCount > Max<ui64>() - ReservedPages - firstPageNo)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "page range [" << firstPageNo << ", +" << pageCount
                << ") overflows internal page reserve");
    }

    return {};
}

NProto::TError InitializeDevice(
    TFastShardContext& ctx,
    const TStorageGroupConfig& config,
    ui32 groupType,
    TDeviceProxy& proxy)
{
    TStorageGroupHeader header;
    header.GroupType = groupType;
    header.PageSize = config.PageSize;
    header.DeviceUUIDHash = HashDeviceUUID(proxy.DeviceUUID);

    NProto::TWriteLogRecordRequest request;
    FillHeaders(config, request.MutableHeaders());
    request.SetLogSequenceNumber(1);

    auto* pg = request.AddPageGroups();
    pg->SetFirstPageNo(0);
    *pg->AddContent() = MakeHeaderPage(header, config.PageSize);

    SILK_INFO("sg init: writing header to %s", proxy.DeviceUUID.c_str());

    return proxy.Write(ctx, request);
}

////////////////////////////////////////////////////////////////////////////////

int LoopFiberMain(TLoopPtr* params) noexcept
{
    auto& loop = **params;
    for (;;) {
        if (WaitFor(loop.Stop, loop.Period)) {
            break;
        }

        if (!loop.Tick()) {
            break;
        }
    }

    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError ShiftToDevice(NProto::TWriteLogRecordRequest& request)
{
    for (auto& pg: *request.MutablePageGroups()) {
        auto error = CheckPageRange(pg.GetFirstPageNo(), pg.ContentSize());
        if (HasError(error)) {
            return error;
        }

        pg.SetFirstPageNo(pg.GetFirstPageNo() + ReservedPages);
    }

    return {};
}

NProto::TError ShiftToDevice(NProto::TReadPagesRequest& request)
{
    for (auto& ref: *request.MutablePageGroupRefs()) {
        auto error = CheckPageRange(ref.GetFirstPageNo(), ref.GetPageCount());
        if (HasError(error)) {
            return error;
        }

        ref.SetFirstPageNo(ref.GetFirstPageNo() + ReservedPages);
    }

    return {};
}

NProto::TError ShiftToClient(TVector<TPageGroup>* pageGroups)
{
    for (auto& pg: *pageGroups) {
        if (pg.FirstPageNo < ReservedPages) {
            const ui64 pageNo = pg.FirstPageNo;
            pageGroups->clear();
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "device answered with reserved page " << pageNo);
        }

        pg.FirstPageNo -= ReservedPages;
    }

    return {};
}

NProto::TError ValidateDevice(
    TFastShardContext& ctx,
    const TStorageGroupConfig& config,
    ui32 groupType,
    TDeviceProxy& proxy)
{
    NProto::TReadPagesRequest request;
    FillHeaders(config, request.MutableHeaders());

    auto* ref = request.AddPageGroupRefs();
    ref->SetPageSize(config.PageSize);
    ref->SetFirstPageNo(0);
    ref->SetPageCount(1);

    NProto::TReadPagesResponse response;
    auto error = proxy.Read(ctx, request, &response);
    if (HasError(error)) {
        return error;
    }

    const auto& groups = response.GetPageGroups();
    if (groups.size() != 1 || groups.Get(0).ContentSize() != 1) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header read returned "
                << groups.size() << " page groups instead of one page");
    }

    const auto& content = groups.Get(0).GetContent(0);
    if (content.size() != config.PageSize) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header page is " << content.size()
                << " bytes instead of " << config.PageSize);
    }

    if (IsAllZeroes(content.data(), content.size())) {
        return InitializeDevice(ctx, config, groupType, proxy);
    }

    TStorageGroupHeader header;
    std::memcpy(&header, content.data(), sizeof(header));

    if (header.MagicNumber != TStorageGroupHeader::Magic ||
        header.Version != TStorageGroupHeader::CurrentVersion ||
        header.GroupType != groupType ||
        header.PageSize != config.PageSize ||
        header.DeviceUUIDHash != HashDeviceUUID(proxy.DeviceUUID))
    {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header mismatch: " << header
                << ", expected page size " << config.PageSize);
    }

    proxy.SeedLsn(response.GetLastAckedLogSequenceNumber());
    return {};
}

////////////////////////////////////////////////////////////////////////////////

void TGroupHealth::Fail(const NProto::TError& error)
{
    {
        std::lock_guard g(Mutex);
        Error = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "storage group broken: " << FormatError(error));

        SILK_ERROR("%s", FormatError(Error).c_str());
    }

    BrokenFlag.store(true, std::memory_order_release);
}

NProto::TError TGroupHealth::GetError() const
{
    std::lock_guard g(Mutex);
    return Error;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError TGroupState::CheckReady() const
{
    if (!Initialized) {
        return MakeError(E_REJECTED, "storage group is not initialized");
    }

    if (Health.IsBroken()) {
        return Health.GetError();
    }

    return {};
}

void TGroupState::Stop()
{
    InflightTracker.Stop();

    // Nothing starts a loop once Init is drained.
    for (const auto& loop: Loops) {
        loop->Stop.set();
    }

    for (const auto& loop: Loops) {
        Y_UNUSED(loop->Done.wait());
    }
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TLoopPtr> RunLoop(TDuration period, std::function<bool()> tick)
{
    auto loop = std::make_shared<TLoop>();
    loop->Period = period;
    loop->Tick = std::move(tick);

    const int r =
        silk::FiberScheduler::run(LoopFiberMain, TLoopPtr(loop), &loop->Done);
    if (r) {
        return MakeSpawnError(r);
    }

    return loop;
}

////////////////////////////////////////////////////////////////////////////////

void PushLowWatermark(
    TFastShardContext& ctx,
    TGroupHealth& health,
    TDeviceProxy& proxy,
    ui64 lsn)
{
    auto error = proxy.AdvanceLsnLowWatermark(ctx, lsn);
    if (!HasError(error)) {
        return;
    }

    if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
        health.Fail(error);
        return;
    }

    SILK_WARN("sg low watermark not delivered: %s", FormatError(error).c_str());
}

void RenewSession(
    TFastShardContext& ctx,
    TGroupHealth& health,
    TDeviceProxy& proxy)
{
    auto error = proxy.Acquire(ctx);
    if (!HasError(error)) {
        return;
    }

    const auto kind = GetErrorKind(error);
    if (kind == EErrorKind::ErrorSession || kind == EErrorKind::ErrorFatal) {
        health.Fail(error);
        return;
    }

    SILK_WARN("sg session not renewed: %s", FormatError(error).c_str());
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
