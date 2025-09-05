#include "wait_for_fresh_devices_action.h"

#include "client_factory.h"
#include "helpers.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TWaitForFreshDevicesActionRunner::TWaitForFreshDevicesActionRunner(
        TLog& log,
        const TAliasedVolumes& aliasedVolumes,
        ILoggingServicePtr logging,
        IClientFactory& clientFactory,
        TTestContext& testContext)
    : Log(log)
    , AliasedVolumes(aliasedVolumes)
    , Logging(std::move(logging))
    , ClientFactory(clientFactory)
    , TestContext(testContext)
{}

int TWaitForFreshDevicesActionRunner::Run(
    const NProto::TActionGraph::TWaitForFreshDevicesAction& action)
{
    const TDuration waitDuration = TDuration::Seconds(5);

    TestContext.Client = ClientFactory.CreateClient({}, action.GetName());
    TestContext.Client->Start();

    const TString& volumeName =
        AliasedVolumes.ResolveAlias(action.GetVolumeName());

    STORAGE_INFO("Start waiting " << volumeName.Quote());

    NProto::TDescribeVolumeRequest describeVolumeRequest;
    describeVolumeRequest.SetDiskId(volumeName);

    while (!TestContext.ShouldStop) {
        STORAGE_INFO("Describe volume: " << volumeName.Quote());
        NProto::TDescribeVolumeResponse response = WaitForCompletion(
            "DescribeVolume",
            TestContext.Client->DescribeVolume(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TDescribeVolumeRequest>(
                    describeVolumeRequest)),
            {});

        if (HasError(response)) {
            STORAGE_ERROR(
                "DescribeVolume failed: " << FormatError(response.GetError()));

            return EC_WAIT_FRESH_DEVICES_ACTION_FAILED;
        }

        const ui32 freshDevicesCount = response.GetVolume().FreshDeviceIdsSize();
        STORAGE_INFO("fresh devices count: " << freshDevicesCount);

        if (!freshDevicesCount) {
            STORAGE_INFO("done");
            break;
        }

        STORAGE_INFO("wait for " << FormatDuration(waitDuration));

        Sleep(waitDuration);
    }

    return 0;
}

}   // namespace NCloud::NBlockStore::NLoadTest
