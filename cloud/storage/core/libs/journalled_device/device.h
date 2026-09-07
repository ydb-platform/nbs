#pragma once

#include "public.h"

#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IDevice
{
    virtual ~IDevice() = default;

    [[nodiscard]] virtual auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WritePages(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> = 0;
};

}   // namespace NCloud::NJournalled
