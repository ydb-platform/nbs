#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDevice : public IStartable
{
    virtual ~IJournalledDevice() = default;

    [[nodiscard]] virtual auto ReadPages(
        TInstant now,
        NCloud::NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WriteLogRecord(
        TInstant now,
        NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual auto ReadJournalTail(
        TInstant now,
        NCloud::NProto::TReadJournalTailRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadJournalTailResponse> = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        TInstant now,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDevice
{
    virtual ~IDevice() = default;

    [[nodiscard]] virtual auto ReadPages(
        TInstant now,
        NCloud::NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WritePages(
        TInstant now,
        NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> = 0;
};

using IDevicePtr = std::shared_ptr<IDevice>;

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV2(
    IKeyBufferStorePtr logMetaStore,
    IPageStorePtr logDataStore,
    IDevicePtr device);

}   // namespace NCloud::NJournalled
