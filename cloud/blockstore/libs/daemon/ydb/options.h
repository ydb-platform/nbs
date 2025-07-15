#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/options.h>

#include <cloud/storage/core/libs/kikimr/options.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsYdb final
    : public TOptionsCommon
    , public NCloud::NStorage::TOptionsYdbBase
{
    TString StorageConfig;
    TString StatsUploadConfig;
    TString FeaturesConfig;
    TString LogbrokerConfig;
    TString NotifyConfig;
    TString IamConfig;
    TString KmsConfig;
    TString RootKmsConfig;
    TString ComputeConfig;

    TOptionsYdb();

    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
