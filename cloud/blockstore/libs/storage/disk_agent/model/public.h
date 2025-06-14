#pragma once

#include <util/generic/strbuf.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceClient;
using TDeviceClientPtr = std::shared_ptr<TDeviceClient>;

class TDiskAgentConfig;
using TDiskAgentConfigPtr = std::shared_ptr<TDiskAgentConfig>;

class IMultiAgentWriteHandler;
using IMultiAgentWriteHandlerPtr = std::shared_ptr<IMultiAgentWriteHandler>;

constexpr TStringBuf BackgroundOpsClientId = "migration";
constexpr TStringBuf CheckHealthClientId = "check-health";
constexpr TStringBuf AnyWriterClientId = "any-writer";
constexpr TStringBuf ShadowDiskClientId = "shadow-disk-client";
constexpr TStringBuf CheckRangeClientId = "check-range";
constexpr TStringBuf CopyVolumeClientId = "copy-volume-client";
constexpr TStringBuf AnyReaderClientId = "any-reader";

}   // namespace NCloud::NBlockStore::NStorage
