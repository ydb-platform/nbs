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
    // Timeout for CMS config loading.
    TDuration RegistrationTimeout;

    // Timeout for dynamic node registration via discovery service
    TDuration DynamicNodeRegistrationTimeout;

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
