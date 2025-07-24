#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigTest)
{
    Y_UNIT_TEST(ShouldUseAioAsDefault)
    {
        TLocalFileStoreConfig config;

        const TFileIOConfig fileIO = config.GetFileIOConfig();
        UNIT_ASSERT(std::holds_alternative<TAioConfig>(fileIO));
    }
}

}   // namespace NCloud::NFileStore
