#pragma once

#include "public.h"

#include <cloud/blockstore/config/local_nvme.pb.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>
#include <util/stream/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeConfig
{
private:
    const NProto::TLocalNVMeConfig Config;

public:
    explicit TLocalNVMeConfig(NProto::TLocalNVMeConfig config);

    [[nodiscard]] TString GetNVMeDevicesCacheFile() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore
