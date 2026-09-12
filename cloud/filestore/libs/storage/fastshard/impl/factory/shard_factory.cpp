#include "shard_factory.h"

#include "group_factory.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/mem/memshard.h>
#include <cloud/filestore/libs/storage/fastshard/impl/hash_table_index/shard.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileSystemShardFactory final: public IFileSystemShardFactory
{
private:
    const bool RuntimeEnabled;

public:
    explicit TFileSystemShardFactory(bool runtimeEnabled)
        : RuntimeEnabled(runtimeEnabled)
    {}

    IFileSystemShardPtr CreateShard(
        const TString& fileSystemId,
        const NProtoPrivate::TFastShardConfig& config,
        ui32 shardNo,
        ui64 generation) override
    {
        if (!config.HasPersistentConfig()) {
            return CreateMemFileSystemShard(shardNo, config.GetMemConfig());
        }

        if (!RuntimeEnabled) {
            return CreateFileSystemShardStub();
        }

        return CreateHashTableIndexFileSystemShard(
            fileSystemId,
            shardNo,
            generation,
            CreateStorageGroupFactory(),
            config.GetPersistentConfig());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardFactoryPtr CreateFileSystemShardFactory(bool runtimeEnabled)
{
    return std::make_shared<TFileSystemShardFactory>(runtimeEnabled);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
