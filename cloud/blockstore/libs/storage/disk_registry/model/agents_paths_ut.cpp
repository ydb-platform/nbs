#include "agents_paths.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TAgentConfig CreateAgentConfig(
    const TString& agentId,
    const THashMap<TString, NProto::EPathAttachState>& pathStates)
{
    NProto::TAgentConfig config;
    config.SetAgentId(agentId);
    for (const auto& [path, state]: pathStates) {
        (*config.MutablePathAttachStates())[path] = state;
    }
    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAgentsPathsTest)
{

    Y_UNIT_TEST(ShouldInitializeFromAgentConfigs)
    {
        TVector<NProto::TAgentConfig> agents;

        // Agent1 with mixed states
        agents.push_back(CreateAgentConfig("agent1", {
            {"/path1", NProto::PATH_ATTACH_STATE_ATTACHING},
            {"/path2", NProto::PATH_ATTACH_STATE_ATTACHED},
            {"/path3", NProto::PATH_ATTACH_STATE_ATTACHING}
        }));

        // Agent2 with only attaching paths
        agents.push_back(CreateAgentConfig("agent2", {
            {"/path4", NProto::PATH_ATTACH_STATE_ATTACHING}
        }));

        // Agent3 with no attaching paths
        agents.push_back(CreateAgentConfig("agent3", {
            {"/path5", NProto::PATH_ATTACH_STATE_ATTACHED},
            {"/path6", NProto::PATH_ATTACH_STATE_DETACHED}
        }));

        TAgentsPaths agentsPaths(agents);
        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Only ATTACHING paths should be added
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.size());

        UNIT_ASSERT(pathsToAttach.contains("agent1"));
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path3"));
        UNIT_ASSERT(!pathsToAttach.at("agent1").contains("/path2"));

        UNIT_ASSERT(pathsToAttach.contains("agent2"));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent2").size());
        UNIT_ASSERT(pathsToAttach.at("agent2").contains("/path4"));

        // Agent3 should not be in the map
        UNIT_ASSERT(!pathsToAttach.contains("agent3"));
    }

    Y_UNIT_TEST(ShouldInitializeFromEmptyAgentList)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();
        UNIT_ASSERT_VALUES_EQUAL(0, pathsToAttach.size());
    }

    Y_UNIT_TEST(ShouldAddPathToAttach)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent1", "/path2");
        agentsPaths.AddPathToAttach("agent2", "/path3");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.size());
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path2"));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent2").size());
        UNIT_ASSERT(pathsToAttach.at("agent2").contains("/path3"));
    }

    Y_UNIT_TEST(ShouldHandleDuplicatePaths)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        // Add same path multiple times
        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent1", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Should only be stored once (THashSet behavior)
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
    }

    Y_UNIT_TEST(ShouldDeletePathToAttach)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent1", "/path2");
        agentsPaths.AddPathToAttach("agent2", "/path3");

        // Delete one path from agent1
        agentsPaths.DeletePathToAttach("agent1", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.size());
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(!pathsToAttach.at("agent1").contains("/path1"));
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path2"));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent2").size());
    }

    Y_UNIT_TEST(ShouldRemoveAgentWhenLastPathDeleted)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent2", "/path2");

        // Delete the only path from agent1
        agentsPaths.DeletePathToAttach("agent1", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Agent1 should be removed from the map
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT(!pathsToAttach.contains("agent1"));
        UNIT_ASSERT(pathsToAttach.contains("agent2"));
    }

    Y_UNIT_TEST(ShouldHandleDeleteNonExistentPath)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");

        // Try to delete non-existent path
        agentsPaths.DeletePathToAttach("agent1", "/nonexistent");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Should not affect existing paths
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
    }

    Y_UNIT_TEST(ShouldHandleDeleteFromNonExistentAgent)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");

        // Try to delete from non-existent agent - should not crash
        agentsPaths.DeletePathToAttach("nonexistent", "/path");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Should not affect existing data
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT(pathsToAttach.contains("agent1"));
    }

    Y_UNIT_TEST(ShouldDeleteAgent)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent1", "/path2");
        agentsPaths.AddPathToAttach("agent2", "/path3");
        agentsPaths.AddPathToAttach("agent3", "/path4");

        // Delete agent1
        agentsPaths.DeleteAgent("agent1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Agent1 should be completely removed
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.size());
        UNIT_ASSERT(!pathsToAttach.contains("agent1"));
        UNIT_ASSERT(pathsToAttach.contains("agent2"));
        UNIT_ASSERT(pathsToAttach.contains("agent3"));
    }

    Y_UNIT_TEST(ShouldHandleDeleteNonExistentAgent)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        agentsPaths.AddPathToAttach("agent1", "/path1");

        // Delete non-existent agent - should not crash
        agentsPaths.DeleteAgent("nonexistent");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Should not affect existing data
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT(pathsToAttach.contains("agent1"));
    }

    Y_UNIT_TEST(ShouldGetPathsToAttach)
    {
        TVector<NProto::TAgentConfig> agents;
        agents.push_back(CreateAgentConfig("agent1", {
            {"/path1", NProto::PATH_ATTACH_STATE_ATTACHING}
        }));

        TAgentsPaths agentsPaths(agents);

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Verify we get a const reference
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT(pathsToAttach.contains("agent1"));
    }

    Y_UNIT_TEST(ShouldHandleComplexScenario)
    {
        // Initialize with some agents
        TVector<NProto::TAgentConfig> agents;
        agents.push_back(CreateAgentConfig("agent1", {
            {"/path1", NProto::PATH_ATTACH_STATE_ATTACHING},
            {"/path2", NProto::PATH_ATTACH_STATE_ATTACHED}
        }));
        agents.push_back(CreateAgentConfig("agent2", {
            {"/path3", NProto::PATH_ATTACH_STATE_ATTACHING}
        }));

        TAgentsPaths agentsPaths(agents);

        // Add more paths
        agentsPaths.AddPathToAttach("agent1", "/path4");
        agentsPaths.AddPathToAttach("agent3", "/path5");

        // Delete some paths
        agentsPaths.DeletePathToAttach("agent1", "/path1");
        agentsPaths.DeletePathToAttach("agent2", "/path3");

        // Delete an agent
        agentsPaths.DeleteAgent("agent2");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        // Verify final state
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.size());

        UNIT_ASSERT(pathsToAttach.contains("agent1"));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path4"));

        UNIT_ASSERT(pathsToAttach.contains("agent3"));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("agent3").size());
        UNIT_ASSERT(pathsToAttach.at("agent3").contains("/path5"));

        UNIT_ASSERT(!pathsToAttach.contains("agent2"));
    }

    Y_UNIT_TEST(ShouldHandleEmptyPathStrings)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        // Add empty path
        agentsPaths.AddPathToAttach("agent1", "");
        agentsPaths.AddPathToAttach("agent1", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT_VALUES_EQUAL(2, pathsToAttach.at("agent1").size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains(""));
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
    }

    Y_UNIT_TEST(ShouldHandleEmptyAgentId)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        // Add path with empty agent id
        agentsPaths.AddPathToAttach("", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.size());
        UNIT_ASSERT(pathsToAttach.contains(""));
        UNIT_ASSERT_VALUES_EQUAL(1, pathsToAttach.at("").size());
    }

    Y_UNIT_TEST(ShouldHandleMultipleAgentsWithSamePaths)
    {
        TVector<NProto::TAgentConfig> agents;
        TAgentsPaths agentsPaths(agents);

        // Different agents can have the same path
        agentsPaths.AddPathToAttach("agent1", "/path1");
        agentsPaths.AddPathToAttach("agent2", "/path1");
        agentsPaths.AddPathToAttach("agent3", "/path1");

        const auto& pathsToAttach = agentsPaths.GetPathsToAttach();

        UNIT_ASSERT_VALUES_EQUAL(3, pathsToAttach.size());
        UNIT_ASSERT(pathsToAttach.at("agent1").contains("/path1"));
        UNIT_ASSERT(pathsToAttach.at("agent2").contains("/path1"));
        UNIT_ASSERT(pathsToAttach.at("agent3").contains("/path1"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
