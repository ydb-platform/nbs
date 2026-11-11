#include "storage_group_proxy.h"

#include "storage_group_helpers.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::TFastShardContext;

////////////////////////////////////////////////////////////////////////////////

NProto::TError TDeviceProxy::Write(
    TFastShardContext& ctx,
    const NProto::TWriteLogRecordRequest& request)
{
    auto response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            NProto::TWriteLogRecordRequest deviceRequest = request;
            deviceRequest.SetDeviceUUID(DeviceUUID);
            return StorageNode->WriteLogRecord(
                ctx,
                std::move(deviceRequest));
        });

    if (HasError(response)) {
        return MakeProxyError(response.GetError(), "write");
    }

    // Acked only moves up, so we do not care about ordering here
    Acked.advance(request.GetLogSequenceNumber());
    return {};
}

NProto::TError TDeviceProxy::Read(
    TFastShardContext& ctx,
    const NProto::TReadPagesRequest& request,
    NProto::TReadPagesResponse* response)
{
    *response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            NProto::TReadPagesRequest deviceRequest = request;
            deviceRequest.SetDeviceUUID(DeviceUUID);
            return StorageNode->ReadPages(ctx, std::move(deviceRequest));
        });

    if (HasError(*response)) {
        return MakeProxyError(response->GetError(), "read");
    }

    return {};
}

NProto::TError TDeviceProxy::ReadJournalTail(
    TFastShardContext& ctx,
    ui64 afterLsn,
    ui32 maxRecords,
    NProto::TReadJournalTailResponse* response)
{
    NProto::TReadJournalTailRequest request;
    FillHeaders(Config, request.MutableHeaders());
    request.SetAfterLogSequenceNumber(afterLsn);
    request.SetMaxRecordCount(maxRecords);

    *response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            NProto::TReadJournalTailRequest deviceRequest = request;
            deviceRequest.SetDeviceUUID(DeviceUUID);
            return StorageNode->ReadJournalTail(
                ctx,
                std::move(deviceRequest));
        });

    if (HasError(*response)) {
        return MakeProxyError(
            response->GetError(),
            TStringBuilder() << "read tail after " << afterLsn);
    }

    return {};
}

NProto::TError TDeviceProxy::AdvanceLsnLowWatermark(
    TFastShardContext& ctx,
    ui64 lsn)
{
    NProto::TAdvanceLsnLowWatermarkRequest request;
    FillHeaders(Config, request.MutableHeaders());
    request.SetDeviceUUID(DeviceUUID);
    request.SetLsnLowWatermark(lsn);

    auto response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            return StorageNode->AdvanceLsnLowWatermark(ctx, request);
        });

    if (HasError(response)) {
        return MakeProxyError(
            response.GetError(),
            TStringBuilder() << "advance lwm to " << lsn);
    }

    return {};
}

NProto::TError TDeviceProxy::Acquire(TFastShardContext& ctx)
{
    NProto::TAcquireDevicesRequest request;
    FillHeaders(Config, request.MutableHeaders());
    request.SetGeneration(Config.AcquireGeneration);
    request.AddDeviceUUIDs(DeviceUUID);

    auto response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            return StorageNode->AcquireDevices(ctx, request);
        });

    if (HasError(response)) {
        return MakeProxyError(response.GetError(), "acquire");
    }

    return {};
}

NProto::TError TDeviceProxy::Release(TFastShardContext& ctx)
{
    NProto::TReleaseDevicesRequest request;
    FillHeaders(Config, request.MutableHeaders());
    request.AddDeviceUUIDs(DeviceUUID);

    auto response = CallWithRetries(
        ctx,
        Config.RetryPolicy,
        *Timer,
        [&]
        {
            return StorageNode->ReleaseDevices(ctx, request);
        });

    if (HasError(response)) {
        return MakeProxyError(response.GetError(), "release");
    }

    return {};
}

NProto::TError TDeviceProxy::MakeProxyError(
    const NProto::TError& error,
    TStringBuf what) const
{
    return MakeError(
        error.GetCode(),
        TStringBuilder()
            << "[" << DeviceUUID << "] " << what
            << " failed: " << error.GetMessage(),
        error.GetFlags());
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
