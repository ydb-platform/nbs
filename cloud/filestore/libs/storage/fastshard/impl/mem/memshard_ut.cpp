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
        auto s = CreateMemFileSystemShard();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            s->CreateNode({}).GetValueSync().GetError().GetCode());
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
