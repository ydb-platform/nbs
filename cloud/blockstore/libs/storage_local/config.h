#pragma once

#include "public.h"

#include <cloud/blockstore/config/storage_local.pb.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TLocalStorageConfig
{
public:
    NProto::TLocalStorageConfig Config;

public:
    TLocalStorageConfig() = default;
    explicit TLocalStorageConfig(NProto::TLocalStorageConfig config)
        : Config(std::move(config))
    {}

    bool GetEnabled() const;
    bool GetDirectIoDisabled() const;
    bool GetSingleQueue() const;
    NProto::ELocalStorageBackend GetBackend() const;
    i32 GetSubmissionQueueThreadCount() const;
    i32 GetCompletionQueueThreadCount() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NServer
