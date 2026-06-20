#include "shard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorUnsafeShardTest)
{
    Y_UNIT_TEST(ShouldServeBasicOps)
    {
        constexpr ui32 ShardNo = 77;

        NProtoPrivate::TPersistentFastShardConfig config;
        // TODO: fill config
        auto s = CreateMirrorUnsafeFileSystemShard(ShardNo, config);
        Y_UNUSED(s);
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
