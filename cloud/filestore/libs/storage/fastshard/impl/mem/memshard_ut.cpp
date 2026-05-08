#include "memshard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMemShardTest)
{
    Y_UNIT_TEST(ShouldCreateUnlinkFiles)
    {
        constexpr ui32 ShardNo = 77;

        auto s = CreateMemFileSystemShard(ShardNo);
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            s->CreateNode({}).GetValueSync().GetError().GetCode());

        // TODO(#5643): write uts.
        //
        // We have some uts in the tablet ut test suite - not sure whether we
        // really need the same coverage here. But covering some basic stuff
        // would be nice.
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
