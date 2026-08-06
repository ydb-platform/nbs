#pragma once

namespace testing {

class Environment;

}   // namespace testing

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns a gtest environment that calls silk::initialize() +
 * FiberScheduler::initialize() in SetUp and destroys both in TearDown.
 *
 * Every ut binary that runs test bodies inside FiberScheduler::run
 * should register exactly one instance at file scope:
 *
 *     [[maybe_unused]] auto* const gEnv =
 *         ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());
 *
 * @return - Owning pointer, hand to ::testing::AddGlobalTestEnvironment.
 *           In stub builds this returns nullptr, which gtest ignores.
 */
::testing::Environment* MakeSilkTestEnv();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
