#pragma once

#include "cloud/storage/core/libs/common/proto_helpers.h"

#include <cloud/blockstore/libs/storage/volume_balancer/volume_balancer_state.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

TDiagnosticsConfigPtr CreateDiagnosticsConfig()
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
    return std::make_shared<TDiagnosticsConfig>(config);
}

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage
