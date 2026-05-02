#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/options.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

// IMPORTANT: do not delete "null", "local", or "kikimr" annotations as they are
// used for enum serialization
enum class EServiceKind
{
    Null   /* "null"   */ ,
    Local  /* "local"  */ ,
    Kikimr /* "kikimr" */ ,
};

////////////////////////////////////////////////////////////////////////////////

struct TOptionsCommon
    : public NCloud::NStorage::TOptionsYdbBase
{
    TString AppConfig;
    TString StorageConfig;
    TString FeaturesConfig;

    ui32 NodeRegistrationMaxAttempts = 0;
    TDuration NodeRegistrationTimeout;
    TDuration NodeRegistrationErrorTimeout;

    EServiceKind Service = EServiceKind::Null;

    bool DisableLocalService = false;

    TOptionsCommon();

    void Parse(int argc, char** argv) override;
};

} // NCloud::NFileStore::NDaemon
