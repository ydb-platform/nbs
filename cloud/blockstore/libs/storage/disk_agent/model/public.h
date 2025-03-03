#pragma once

#include <util/generic/strbuf.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceClient;
using TDeviceClientPtr = std::shared_ptr<TDeviceClient>;

class TDiskAgentConfig;
using TDiskAgentConfigPtr = std::shared_ptr<TDiskAgentConfig>;

constexpr TStringBuf BackgroundOpsClientId = "migration";
constexpr TStringBuf CheckHealthClientId = "check-health";
constexpr TStringBuf AnyWriterClientId = "any-writer";
constexpr TStringBuf ShadowDiskClientId = "shadow-disk-client";
constexpr TStringBuf CheckRangeClientId = "check-range";

}   // namespace NCloud::NBlockStore::NStorage
