#pragma once

#include <cloud/fastshard/journal/iface/public.h>
#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/string.h>

#include <atomic>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceLayout
{
    ui32 PageSize = 4096;
    ui64 LogMetaPageCount = 400;
    ui64 LogDataPageCount = 1600;
    ui64 DataPageCount = 6000;
};

// Requests taken, per method.
struct TRequestCounters
{
#define SN_DECLARE_COUNTER(name, ...) std::atomic<ui32> name = 0;
    SN_METHODS(SN_DECLARE_COUNTER)
#undef SN_DECLARE_COUNTER
};

struct TJournalledStorageNode
    : public IStorageNode
    , public IStartable
{
    const TString DeviceUUID;
    const TJournalledDeviceLayout Layout;
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const NJournalled::IDevicePtr LogMetaDevice;
    const NJournalled::IDevicePtr LogDataDevice;
    const NJournalled::IDevicePtr DataDevice;

    NJournalled::IJournalledDevicePtr Device;
    TRequestCounters Counters;

    bool Started = false;

    TJournalledStorageNode(
        TString deviceUUID,
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        TJournalledDeviceLayout layout = {});

    void Start() override;
    void Stop() override;

#define SN_DECLARE_METHOD(name, ...)                                           \
    NProto::T##name##Response name(NProto::T##name##Request request) override; \
    // SN_DECLARE_METHOD

    SN_METHODS(SN_DECLARE_METHOD)

#undef SN_DECLARE_METHOD
};

}   // namespace NCloud::NFastShard
