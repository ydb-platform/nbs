#pragma once

#include "public.h"

#include <cloud/blockstore/config/local_nvme.pb.h>

#include <util/generic/fwd.h>

#include <variant>

class IOutputStream;

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeConfig
{
private:
    const NProto::TLocalNVMeConfig Proto;

public:
    explicit TLocalNVMeConfig(NProto::TLocalNVMeConfig proto);
    ~TLocalNVMeConfig();

    [[nodiscard]] TString GetDevicesSourceUri() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore
