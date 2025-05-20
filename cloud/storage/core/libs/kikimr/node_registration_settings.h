#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

// This struct encapsulates parameters required for node registration in
// ydb cluster and initial configuration acquiring from cms. Right now these
// parameters are actually the part of TServerConfig.
// TODO: https://github.com/ydb-platform/nbs/issues/1656
struct TNodeRegistrationSettings
{
    ui32 MaxAttempts = 0;
    TDuration ErrorTimeout;
    TDuration RegistrationTimeout;

    TDuration LoadConfigsFromCmsRetryMinDelay;
    TDuration LoadConfigsFromCmsRetryMaxDelay;
    TDuration LoadConfigsFromCmsTotalTimeout;

    TString PathToGrpcCaFile;
    TString PathToGrpcCertFile;
    TString PathToGrpcPrivateKeyFile;

    TString NodeRegistrationToken;

    TString NodeType;
};

}   // namespace NCloud::NStorage
