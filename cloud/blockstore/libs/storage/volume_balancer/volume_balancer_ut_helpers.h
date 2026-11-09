#pragma once

#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

TDiagnosticsConfigConstPtr CreateDiagnosticsConfig()
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

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage
