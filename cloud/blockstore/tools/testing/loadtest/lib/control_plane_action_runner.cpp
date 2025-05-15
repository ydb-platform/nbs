#include "control_plane_action_runner.h"

#include "helpers.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/set.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TControlPlaneActionRunner::TControlPlaneActionRunner(
        TLog& log,
        TAppContext& appContext,
        const TAliasedVolumes& aliasedVolumes,
        TVolumeInfos& volumeInfos,
        IBlockStorePtr client,
        TTestContext& testContext)
    : Log(log)
    , AppContext(appContext)
    , AliasedVolumes(aliasedVolumes)
    , VolumeInfos(volumeInfos)
    , Client(std::move(client))
    , TestContext(testContext)
{
}

int TControlPlaneActionRunner::Run(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    try {
        switch (action.GetRequestCase()) {
            case NProto::TActionGraph::TControlPlaneAction::kCreateVolumeRequest:
            {
                RunCreateVolumeAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDestroyVolumeRequest:
            {
                RunDestroyVolumeAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kCreateCheckpointRequest:
            {
                RunCreateCheckpointAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDeleteCheckpointRequest:
            {
                RunDeleteCheckpointAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kStatVolumeRequest:
            {
                RunStatVolumeAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDiscoverInstancesRequest:
            {
                RunDiscoverInstancesAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kResizeVolumeRequest:
            {
                RunResizeVolumeAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kCreatePlacementGroupRequest:
            {
                RunCreatePlacementGroupAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDestroyPlacementGroupRequest:
            {
                RunDestroyPlacementGroupAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDescribePlacementGroupRequest:
            {
                RunDescribePlacementGroupAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kDescribeVolumeRequest:
            {
                RunDescribeVolumeAction(action);
                break;
            }

            case NProto::TActionGraph::TControlPlaneAction::kCmsRemoveHostRequest:
            {
                return RunCmsRemoveHostAction(action);
            }

            case NProto::TActionGraph::TControlPlaneAction::kCmsRemoveDeviceRequest:
            {
                return RunCmsRemoveDeviceAction(action);
            }

            case NProto::TActionGraph::TControlPlaneAction::kReplaceDevicesRequest:
            {
                return RunReplaceDevicesRequest(action);
            }

            default: Y_ABORT_UNLESS(0);
        }
    } catch (...) {
        STORAGE_ERROR("Exception during cpa execution: "
            << CurrentExceptionMessage());
        AppContext.FailedTests.fetch_add(1);
        return EC_CONTROL_PLANE_ACTION_FAILED;
    }

    return 0;
}

void TControlPlaneActionRunner::RunCreateVolumeAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetCreateVolumeRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Create volume: " << request.GetDiskId());
    auto response = WaitForCompletion(
        "CreateVolume",
        Client->CreateVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TCreateVolumeRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDestroyVolumeAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetDestroyVolumeRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Destroy volume: " << request.GetDiskId());
    auto response = WaitForCompletion(
        "DestroyVolume",
        Client->DestroyVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDestroyVolumeRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunCreateCheckpointAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    auto request = std::make_shared<NProto::TCreateCheckpointRequest>();
    request->CopyFrom(action.GetCreateCheckpointRequest());

    const auto volumeName = AliasedVolumes.ResolveAlias(request->GetDiskId());
    request->SetDiskId(volumeName);

    const auto requestId = GetRequestId(*request);

    STORAGE_INFO(
        "Create checkpoint: " << request->GetCheckpointId() <<
        " for volume: " << volumeName);

    auto response = WaitForCompletion(
        "CreateCheckpoint",
        Client->CreateCheckpoint(
            MakeIntrusive<TCallContext>(requestId),
            request
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunResizeVolumeAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    auto request = std::make_shared<NProto::TResizeVolumeRequest>();
    request->CopyFrom(action.GetResizeVolumeRequest());

    const auto volumeName = AliasedVolumes.ResolveAlias(request->GetDiskId());
    request->SetDiskId(volumeName);

    const auto requestId = GetRequestId(*request);

    STORAGE_INFO(
        "ResizeVolume to " << request->GetBlocksCount() << " blocks"
        " for volume: " << volumeName);

    auto response = WaitForCompletion(
        "ResizeVolume",
        Client->ResizeVolume(
            MakeIntrusive<TCallContext>(requestId),
            request
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDeleteCheckpointAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    auto request = std::make_shared<NProto::TDeleteCheckpointRequest>();
    request->CopyFrom(action.GetDeleteCheckpointRequest());

    const auto volumeName = AliasedVolumes.ResolveAlias(request->GetDiskId());
    request->SetDiskId(volumeName);

    const auto requestId = GetRequestId(*request);

    STORAGE_INFO(
        "Delete checkpoint: " << request->GetCheckpointId() <<
        " for volume: " << volumeName);

    auto response = WaitForCompletion(
        "DeleteCheckpoint",
        Client->DeleteCheckpoint(
            MakeIntrusive<TCallContext>(requestId),
            request
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunStatVolumeAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetStatVolumeRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Stat volume: " << request.GetDiskId());
    auto response = WaitForCompletion(
        "StatVolume",
        Client->StatVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TStatVolumeRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDiscoverInstancesAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetDiscoverInstancesRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Discover instances");
    auto response = WaitForCompletion(
        "DiscoverInstances",
        Client->DiscoverInstances(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDiscoverInstancesRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunCreatePlacementGroupAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetCreatePlacementGroupRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Create placement group: " << request.GetGroupId());
    auto response = WaitForCompletion(
        "CreatePlacementGroup",
        Client->CreatePlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TCreatePlacementGroupRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDestroyPlacementGroupAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetDestroyPlacementGroupRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Destroy placement group: " << request.GetGroupId());
    auto response = WaitForCompletion(
        "DestroyPlacementGroup",
        Client->DestroyPlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDestroyPlacementGroupRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDescribePlacementGroupAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetDescribePlacementGroupRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Describe placement group: " << request.GetGroupId());
    auto response = WaitForCompletion(
        "DescribePlacementGroup",
        Client->DescribePlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDescribePlacementGroupRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});
}

void TControlPlaneActionRunner::RunDescribeVolumeAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetDescribeVolumeRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Describe volume: " << request.GetDiskId());
    NProto::TDescribeVolumeResponse response = WaitForCompletion(
        "DescribeVolume",
        Client->DescribeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDescribeVolumeRequest>(request)
        ),
        {}
    );

    NProtobufJson::Proto2Json(response, TestContext.Result, {});

    const auto& d = response.GetVolume().GetDevices();
    TVector<TString> agentIds;
    TVector<TDeviceAddress> devices;
    auto addDevices = [&] (const auto& d) {
        for (const auto& device: d) {
            if (Find(agentIds, device.GetAgentId()) == agentIds.end()) {
                agentIds.push_back(device.GetAgentId());
            }
            devices.push_back({
                device.GetAgentId(),
                device.GetDeviceName(),
                device.GetDeviceUUID(),
            });
        }
    };

    addDevices(d);

    for (const auto& replica: response.GetVolume().GetReplicas()) {
        addDevices(replica.GetDevices());
    }

    if (agentIds) {
        VolumeInfos.UpdateAgentIds(request.GetDiskId(), std::move(agentIds));

        VolumeInfos.UpdateDevices(request.GetDiskId(), std::move(devices));
    }
}

int TControlPlaneActionRunner::RunCmsRemoveHostAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetCmsRemoveHostRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Cms remove host: " << request.GetAgentRef());

    TStringBuf diskId, agentNoStr;
    TStringBuf(request.GetAgentRef()).Split('/', diskId, agentNoStr);

    auto agentNo = FromString<ui32>(agentNoStr);
    TVolumeInfo volumeInfo;
    if (!VolumeInfos.GetVolumeInfo(diskId, &volumeInfo)) {
        STORAGE_WARN("Volume not found: " << diskId);
        return 1;
    }

    if (volumeInfo.AgentIds.size() <= agentNo) {
        STORAGE_WARN("Not enough agents for volume: " << diskId
            << ", agent count: " << volumeInfo.AgentIds.size());
        return 1;
    }

    if (request.GetHard()) {
        return CmsRemoveHostHard(agentNo, requestId, volumeInfo);
    }

    NProto::TCmsActionRequest cmsRequest;
    auto* cmsAction = cmsRequest.MutableActions()->Add();
    *cmsAction->MutableHost() = volumeInfo.AgentIds[agentNo];
    cmsAction->SetType(NProto::TAction::REMOVE_HOST);

    STORAGE_INFO("Cms remove host (resolved): " << cmsAction->GetHost());

    const auto timeout = TDuration::Seconds(request.GetTimeout());
    const auto retryTimeout = request.GetRetryTimeout()
        ? TDuration::Seconds(request.GetRetryTimeout())
        : TDuration::Seconds(5);
    const auto endTime = timeout.ToDeadLine();

    auto cmsActionRequest = std::make_shared<NProto::TCmsActionRequest>(std::move(cmsRequest));

    for (;;) {
        NProto::TCmsActionResponse response = WaitForCompletion(
            "CmsAction",
            Client->CmsAction(
                MakeIntrusive<TCallContext>(requestId),
                cmsActionRequest
            ),
            {}
        );

        NProtobufJson::Proto2Json(response, TestContext.Result, {});

        Y_ENSURE(response.ActionResultsSize() == 1);
        auto code = response.GetActionResults(0).GetResult().GetCode();

        if (SUCCEEDED(code) == request.GetSuccessExpected()) {
            break;
        }

        if (code == E_TRY_AGAIN && Now() < endTime) {
            Sleep(retryTimeout);

            STORAGE_INFO("Try again");

            TestContext.Result << "\n";

            continue;
        }

        STORAGE_ERROR("Expected != Actual: "
            << request.GetSuccessExpected() << ", code = "
            << static_cast<EWellKnownResultCodes>(code));
        AppContext.FailedTests.fetch_add(1);

        return EC_CONTROL_PLANE_ACTION_FAILED;
    }

    return CmsRemoveHostHard(agentNo, requestId, volumeInfo);
}

int TControlPlaneActionRunner::CmsRemoveHostHard(
    ui32 agentNo,
    ui64 requestId,
    const TVolumeInfo& volumeInfo)
{
    NPrivateProto::TDiskRegistryChangeStateRequest changeState;
    changeState.SetMessage("loadtest");
    auto* disableAgent = changeState.MutableDisableAgent();
    disableAgent->SetAgentId(volumeInfo.AgentIds[agentNo]);

    TStringStream input;
    changeState.PrintJSON(input);

    NProto::TExecuteActionRequest executeAction;
    executeAction.SetAction("DiskRegistryChangeState");
    executeAction.SetInput(input.Str());

    NProto::TExecuteActionResponse response = WaitForCompletion(
        "ExecuteAction",
        Client->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TExecuteActionRequest>(
                std::move(executeAction)
            )
        ),
        {}
    );

    TString log;
    NProtobufJson::Proto2Json(response, log, {});

    STORAGE_INFO("ExecuteAction result: " << log);

    return 0;
}

int TControlPlaneActionRunner::RunCmsRemoveDeviceAction(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetCmsRemoveDeviceRequest();
    const auto requestId = GetRequestId(request);

    STORAGE_INFO("Cms remove device: " << request.GetDeviceRef());

    TStringBuf diskId, deviceNoStr;
    TStringBuf(request.GetDeviceRef()).Split('/', diskId, deviceNoStr);

    auto deviceNo = FromString<ui32>(deviceNoStr);
    TVolumeInfo volumeInfo;
    if (!VolumeInfos.GetVolumeInfo(diskId, &volumeInfo)) {
        STORAGE_WARN("Volume not found: " << diskId);
        return 1;
    }

    if (volumeInfo.Devices.size() <= deviceNo) {
        STORAGE_WARN("Not enough devices for volume: " << diskId
            << ", device count: " << volumeInfo.Devices.size());
        return 1;
    }

    NProto::TCmsActionRequest cmsRequest;
    auto* cmsAction = cmsRequest.MutableActions()->Add();
    const auto& device = volumeInfo.Devices[deviceNo];
    *cmsAction->MutableDevice() = device.Name;
    *cmsAction->MutableHost() = device.AgentId;
    cmsAction->SetType(NProto::TAction::REMOVE_DEVICE);

    STORAGE_INFO("Cms remove device (resolved): "
        << device.AgentId << "::" << device.Name);

    const auto timeout = TDuration::Seconds(request.GetTimeout());
    const auto retryTimeout = request.GetRetryTimeout()
        ? TDuration::Seconds(request.GetRetryTimeout())
        : TDuration::Seconds(5);
    const auto endTime = timeout.ToDeadLine();

    STORAGE_INFO("timeout: " << timeout << " retryTimeout: " << retryTimeout);

    for (;;) {
        NProto::TCmsActionResponse response = WaitForCompletion(
            "CmsAction",
            Client->CmsAction(
                MakeIntrusive<TCallContext>(requestId),
                std::make_shared<NProto::TCmsActionRequest>(cmsRequest)
            ),
            {}
        );

        NProtobufJson::Proto2Json(response, TestContext.Result, {});

        Y_ENSURE(response.ActionResultsSize() == 1);
        auto code = response.GetActionResults(0).GetResult().GetCode();

        if (FAILED(code) && !request.GetSuccessExpected()) {
            return 0;
        }

        if (SUCCEEDED(code) && request.GetSuccessExpected()) {
            break;
        }

        if (code == E_TRY_AGAIN && Now() < endTime) {
            Sleep(retryTimeout);

            STORAGE_INFO("Try again");

            continue;
        }

        STORAGE_ERROR("Expected != Actual: "
            << request.GetSuccessExpected() << ", code = "
            << static_cast<EWellKnownResultCodes>(code));
        AppContext.FailedTests.fetch_add(1);

        return EC_CONTROL_PLANE_ACTION_FAILED;
    }

    NPrivateProto::TDiskRegistryChangeStateRequest changeState;
    changeState.SetMessage("loadtest");
    auto* changeDeviceState = changeState.MutableChangeDeviceState();
    changeDeviceState->SetDeviceUUID(device.DeviceUUID);
    changeDeviceState->SetState(2); // DEVICE_STATE_ERROR

    TStringStream input;
    changeState.PrintJSON(input);

    NProto::TExecuteActionRequest executeAction;
    executeAction.SetAction("DiskRegistryChangeState");
    executeAction.SetInput(input.Str());

    NProto::TExecuteActionResponse response = WaitForCompletion(
        "ExecuteAction",
        Client->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TExecuteActionRequest>(
                std::move(executeAction)
            )
        ),
        {}
    );

    TString log;
    NProtobufJson::Proto2Json(response, log, {});

    STORAGE_INFO("ExecuteAction result: " << log);

    return 0;
}

int TControlPlaneActionRunner::RunReplaceDevicesRequest(
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    const auto& request = action.GetReplaceDevicesRequest();
    const auto requestId = GetRequestId(request);
    const auto& diskId = request.GetDiskId();

    TVolumeInfo volumeInfo;
    VolumeInfos.GetVolumeInfo(diskId, &volumeInfo);

    NProto::TDescribeVolumeRequest describeVolume;
    describeVolume.SetDiskId(diskId);

    auto describeVolumeResponse = WaitForCompletion(
        "DescribeVolume",
        Client->DescribeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::make_shared<NProto::TDescribeVolumeRequest>(describeVolume)),
        {});

    auto getDeviceUUID = [&](ui64 replicaIdx, ui64 deviceIdx)
    {
        if (replicaIdx == 0) {
            return describeVolumeResponse.GetVolume()
                .GetDevices()[deviceIdx]
                .GetDeviceUUID();
        }
        return describeVolumeResponse.GetVolume()
            .GetReplicas()[replicaIdx - 1]
            .GetDevices()[deviceIdx]
            .GetDeviceUUID();
    };

    for (auto& coordinate: request.GetDevicesToReplace()) {
        auto deviceUUID = getDeviceUUID(
            coordinate.GetReplicaIndex(),
            coordinate.GetDeviceIndex());

        NJson::TJsonValue inputJson;
        inputJson["DiskId"] =
            diskId + "/" + ToString(coordinate.GetReplicaIndex());
        inputJson["DeviceId"] = deviceUUID;

        NProto::TExecuteActionRequest executeAction;
        executeAction.SetAction("ReplaceDevice");
        executeAction.SetInput(NJson::WriteJson(inputJson));

        NProto::TExecuteActionResponse response = WaitForCompletion(
            "ExecuteAction",
            Client->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::make_shared<NProto::TExecuteActionRequest>(
                    std::move(executeAction))),
            {});

        TString log;
        NProtobufJson::Proto2Json(response, log, {});
        STORAGE_INFO("Replace device result: " << log);
    }

    return 0;
}

}   // namespace NCloud::NBlockStore::NLoadTest
