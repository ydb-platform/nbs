#pragma once

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline TDiagnosticsConfigConstPtr CreateDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    ParseProtoTextFromString(
        R"(
        SsdPerfSettings {
          Write {
            Iops: 100
            Bandwidth: 409600
          }
          Read {
            Iops: 100
            Bandwidth: 409600
          }
        })",
        config);
    return std::make_shared<const TDiagnosticsConfig>(config);
}

}   // namespace NCloud::NBlockStore::NStorage
