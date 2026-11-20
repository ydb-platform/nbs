#pragma once

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline TDiagnosticsConfigPtr CreateDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    ParseProtoTextFromString(
        R"(
        BalancerYDBBasedPerfSettings {
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

struct TVolumeLoadProfile
{
    ui64 ReadBlobIops = 0;
    ui64 WriteBlobIops = 0;
    ui64 ReadBlobBW = 0;
    ui64 WriteBlobBW = 0;
};

constexpr TVolumeLoadProfile LoadHeavy{
    .ReadBlobIops = 100,
    .WriteBlobIops = 100,
    .ReadBlobBW = 100 * 4_KB,
    .WriteBlobBW = 100 * 4_KB,
};

constexpr TVolumeLoadProfile LoadModerate{
    .ReadBlobIops = 50,
    .WriteBlobIops = 50,
    .ReadBlobBW = 50 * 4_KB,
    .WriteBlobBW = 50 * 4_KB,
};

constexpr TVolumeLoadProfile LoadLight{
    .ReadBlobIops = 10,
    .WriteBlobIops = 10,
    .ReadBlobBW = 10 * 4_KB,
    .WriteBlobBW = 10 * 4_KB,
};

constexpr TVolumeLoadProfile LoadZero{};

}   // namespace NCloud::NBlockStore::NStorage
