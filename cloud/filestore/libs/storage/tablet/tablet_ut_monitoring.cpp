
#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <contrib/ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Monitoring)
{

    Y_UNIT_TEST(ShouldHandleHttpInfo)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.WaitReady();

        auto response = tablet.GetRemoteHttpInfo();
        // check that it was served by user part of the tablet
        UNIT_ASSERT(response->Html.Contains("Filesystem Id:"));

        //
        // A regular tablet neither shows the fast shard layout link nor
        // serves the page.
        //

        UNIT_ASSERT(!response->Html.Contains("action=fastShardLayout"));

        response = tablet.GetRemoteHttpInfo("action=fastShardLayout");
        UNIT_ASSERT_C(
            response->Html.Contains("not a fast shard"),
            response->Html);
    }

    Y_UNIT_TEST(ShouldHandleHttpInfo_FastShardLayout)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.ConfigureAsShard(
            1 /* shardNo */,
            "main_fs",
            "main_fs_s1",
            false /* directoryCreationInShardsEnabled */,
            TVector<TString>() /* shardIds */,
            NProtoPrivate::TFastShardConfig(),
            true /* isFastShard */);

        tablet.ReconnectPipe();
        tablet.WaitReady();

        auto response = tablet.GetRemoteHttpInfo();
        UNIT_ASSERT_C(
            response->Html.Contains("action=fastShardLayout"),
            response->Html);

        //
        // The tablet is configured with the mem shard, whose layout
        // dump is empty by design - the page contents are covered by
        // the naive mirrored shard's own tests. Here we only check
        // that the action is served and not rejected.
        //

        response = tablet.GetRemoteHttpInfo("action=fastShardLayout");
        UNIT_ASSERT_C(
            !response->Html.Contains("not a fast shard"),
            response->Html);
        UNIT_ASSERT_C(
            !response->Html.Contains("alert-danger"),
            response->Html);
    }

    Y_UNIT_TEST(ShouldHandleHttpInfo_FastShardStatsJson)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.WaitReady();

        //
        // A regular tablet rejects the stats action.
        //

        auto errorResponse =
            tablet.GetRemoteJsonInfo("action=fastShardStatsJson");
        UNIT_ASSERT_C(
            errorResponse->Json.Contains("not a fast shard"),
            errorResponse->Json);

        tablet.ConfigureAsShard(
            1 /* shardNo */,
            "main_fs",
            "main_fs_s1",
            false /* directoryCreationInShardsEnabled */,
            TVector<TString>() /* shardIds */,
            NProtoPrivate::TFastShardConfig(),
            true /* isFastShard */);

        tablet.ReconnectPipe();
        tablet.WaitReady();

        auto response = tablet.GetRemoteJsonInfo("action=fastShardStatsJson");

        NJson::TJsonValue json;
        UNIT_ASSERT_C(
            NJson::ReadJsonTree(response->Json, &json),
            response->Json);
        UNIT_ASSERT_C(!json.Has("error"), response->Json);
        for (const auto* key: {
            "usedNodeCount",
            "totalNodeCount",
            "usedNameCount",
            "totalNameCount",
            "usedHandleCount",
            "totalHandleCount",
            "usedPageCount",
            "totalPageCount",
        })
        {
            UNIT_ASSERT_C(json.Has(key), response->Json);
        }
    }

    Y_UNIT_TEST(ShouldHandleHttpInfo_Diagnostics)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.WaitReady();

        auto response = tablet.GetRemoteHttpInfo("action=diagnostics");
        UNIT_ASSERT(response->Html.Contains("diagnosticsInit"));
        UNIT_ASSERT(response->Html.Contains("Diagnostic Metrics"));
    }
}

}   // namespace NCloud::NFileStore::NStorage
