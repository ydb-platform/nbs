#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeRegistrationSettings
{
    ui32 MaxAttempts = 0;
    TDuration ErrorTimeout;
    TDuration RegistrationTimeout;

    TString PathToGrpcCaFile;
    TString PathToGrpcCertFile;
    TString PathToGrpcPrivateKeyFile;

    TString NodeRegistrationToken;

    TString NodeType;
};

}   // namespace NCloud::NStorage
