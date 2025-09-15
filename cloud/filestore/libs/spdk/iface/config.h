#pragma once

#include "public.h"

#include <cloud/filestore/config/spdk.pb.h>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnvConfig
{
private:
    const NProto::TSpdkEnvConfig Config;

public:
    explicit TSpdkEnvConfig(NProto::TSpdkEnvConfig config = {});

    TString GetCpuMask() const;
    TString GetHugeDir() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore::NSpdk
