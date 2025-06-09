#pragma once

#include <memory>

namespace NCloud::NBlockStore {

namespace NProto {
    class TYdbStatsConfig;
}

namespace NYdbStats {

////////////////////////////////////////////////////////////////////////////////

struct TYdbRowData;

struct IYdbVolumesStatsUploader;
using IYdbVolumesStatsUploaderPtr = std::shared_ptr<IYdbVolumesStatsUploader>;

class TYdbStatsConfig;
using TYdbStatsConfigPtr = std::shared_ptr<TYdbStatsConfig>;

struct IYdbStorage;
using IYdbStoragePtr = std::shared_ptr<IYdbStorage>;

struct TStatsTableScheme;
using TStatsTableSchemePtr = std::shared_ptr<TStatsTableScheme>;

}   // namespace NYdbStats
}   // namespace NCloud::NBlockStore
