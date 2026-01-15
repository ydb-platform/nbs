#pragma once

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui32 MaxChannelsCount = 248;

class TStorageConfig;
using TStorageConfigPtr = std::shared_ptr<TStorageConfig>;

struct TRequestInfo;
using TRequestInfoPtr = TIntrusivePtr<TRequestInfo>;

struct TSystemCounters;
using TSystemCountersPtr = TIntrusivePtr<TSystemCounters>;

}   // namespace NCloud::NFileStore::NStorage
