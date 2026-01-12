#include "helpers.h"

#include <cloud/filestore/config/client.pb.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void EndpointConfigToSessionConfig(
    const NProto::TEndpointConfig& endpointConfig,
    NProto::TSessionConfig *sessionConfig)
{
    sessionConfig->SetFileSystemId(endpointConfig.GetFileSystemId());
    sessionConfig->SetClientId(endpointConfig.GetClientId());

    //
    // TEndpointConfig is proto3 and has no notion of HasXXX for scalars so we
    // need to check whether the value of the field is zero.
    //
    // See #4853
    //

    if (endpointConfig.GetSessionPingTimeout()) {
        sessionConfig->SetSessionPingTimeout(
            endpointConfig.GetSessionPingTimeout());
    }
}

}   // namespace NCloud::NFileStore
