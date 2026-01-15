#pragma once

#include "app_context.h"
#include "aliased_volumes.h"
#include "volume_infos.h"

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TControlPlaneActionRunner
{
private:
    TLog& Log;
    TAppContext& AppContext;
    const TAliasedVolumes& AliasedVolumes;
    TVolumeInfos& VolumeInfos;
    IBlockStorePtr Client;
    TTestContext& TestContext;

public:
    TControlPlaneActionRunner(
        TLog& log,
        TAppContext& appContext,
        const TAliasedVolumes& aliasedVolumes,
        TVolumeInfos& volumeInfos,
        IBlockStorePtr client,
        TTestContext& testContext);

public:
    int Run(const NProto::TActionGraph::TControlPlaneAction& action);

private:
    void RunCreateVolumeAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDestroyVolumeAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunCreateCheckpointAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDeleteCheckpointAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunStatVolumeAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDiscoverInstancesAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunResizeVolumeAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunCreatePlacementGroupAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDestroyPlacementGroupAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDescribePlacementGroupAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    void RunDescribeVolumeAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    int RunCmsRemoveHostAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    int RunCmsRemoveDeviceAction(
        const NProto::TActionGraph::TControlPlaneAction& action);
    int RunReplaceDevicesRequest(
        const NProto::TActionGraph::TControlPlaneAction& action);
    int RunModifyTagsRequest(
        const NProto::TActionGraph::TControlPlaneAction& action);

    int CmsRemoveHostHard(
        ui32 agentNo,
        ui64 requestId,
        const TVolumeInfo& volumeInfo);
};

}   // namespace NCloud::NBlockStore::NLoadTest
