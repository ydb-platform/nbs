#pragma once

#include "public.h"

#include "config.h"

#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/fastshard/journal/iface/public.h>
#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend : public IStartable
{
    virtual ~IServerBackend() = default;

#define SERVICE_DECLARE_METHODS(name, ...)                            \
    [[nodiscard]] virtual auto name(NProto::T##name##Request request) \
        -> NThreading::TFuture<NProto::T##name##Response> = 0;        \
    // JOURNAL_DECLARE_DESCRIBE_REQUEST

    SN_METHODS(SERVICE_DECLARE_METHODS)

#undef SERVICE_DECLARE_METHODS
};

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDeviceFactory
{
    virtual ~IJournalledDeviceFactory() = default;

    virtual TResultOrError<IJournalledDevicePtr> Create(
        const TJournalledDeviceConfig& config) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServerBackendPtr CreateService(
    TExecutorPtr executor,
    IDeviceManagerPtr deviceManager,
    IJournalledDeviceFactoryPtr journalledDeviceFactory,
    TVector<TJournalledDeviceConfig> journalledDeviceConfigs,
    ui32 restoreConcurrency);

}   // namespace NCloud::NJournalled
