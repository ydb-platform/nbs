#pragma once

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/model/public.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NCloud::NBlockStore {

namespace NProto {
class TPartitionConfig;
class TStorageServiceConfig;
}   // namespace NProto

namespace NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStorageConfig;
using TStorageConfigPtr = std::shared_ptr<TStorageConfig>;

struct IWriteBlocksHandler;
using IWriteBlocksHandlerPtr = std::shared_ptr<IWriteBlocksHandler>;

struct IReadBlocksHandler;
using IReadBlocksHandlerPtr = std::shared_ptr<IReadBlocksHandler>;

struct ICompactionPolicy;
using ICompactionPolicyPtr = std::shared_ptr<ICompactionPolicy>;

struct TManuallyPreemptedVolumes;
using TManuallyPreemptedVolumesPtr = std::shared_ptr<TManuallyPreemptedVolumes>;

////////////////////////////////////////////////////////////////////////////////

enum class EStorageAccessMode
{
    Default,
    Repair,
};

}   // namespace NStorage
}   // namespace NCloud::NBlockStore
