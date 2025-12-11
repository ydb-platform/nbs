#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryProxyConfig
{
private:
    const NProto::TDiskRegistryProxyConfig Config;

public:
    explicit TDiskRegistryProxyConfig(
        NProto::TDiskRegistryProxyConfig config = {});

    ui64 GetOwner() const;
    ui64 GetOwnerIdx() const;

    TDuration GetLookupTimeout() const;
    TDuration GetRetryLookupTimeout() const;

    ui64 GetDiskRegistryTabletId() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NStorage
