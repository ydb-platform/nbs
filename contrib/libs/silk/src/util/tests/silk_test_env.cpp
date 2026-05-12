#include <silk/util/init.h>

#include <gtest/gtest.h>

namespace {

class SilkUtilEnvironment : public ::testing::Environment
{
public:
    void SetUp() override { silk::initialize(); }
    void TearDown() override { silk::destroy(); }
};

[[maybe_unused]] auto * const env =
    ::testing::AddGlobalTestEnvironment(new SilkUtilEnvironment);

} // namespace
