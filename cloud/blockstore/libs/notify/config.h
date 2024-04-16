#pragma once

#include "public.h"

#include <cloud/blockstore/config/notify.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

class TNotifyConfig
{
private:
    const NProto::TNotifyConfig Config;

public:
    explicit TNotifyConfig(NProto::TNotifyConfig config);

    TString GetEndpoint() const;
    TString GetCaCertFilename() const;
    NProtoBuf::int32 GetVersion() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NNotify
