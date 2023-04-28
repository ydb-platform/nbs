#pragma once

#include "public.h"

#include <cloud/blockstore/config/logbroker.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

class TLogbrokerConfig
{
private:
    const NProto::TLogbrokerConfig Config;

public:
    explicit TLogbrokerConfig(NProto::TLogbrokerConfig config = {});

    TString GetAddress() const;
    ui32 GetPort() const;
    TString GetDatabase() const;
    bool GetUseLogbrokerCDS() const;
    TString GetCaCertFilename() const;

    TString GetTopic() const;
    TString GetSourceId() const;

    TString GetMetadataServerAddress() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NLogbroker
