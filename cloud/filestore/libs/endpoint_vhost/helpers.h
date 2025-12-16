#pragma once

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TEndpointConfig;
class TSessionConfig;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void EndpointConfigToSessionConfig(
    const NProto::TEndpointConfig& endpointConfig,
    NProto::TSessionConfig *sessionConfig);

}   // namespace NCloud::NFileStore
