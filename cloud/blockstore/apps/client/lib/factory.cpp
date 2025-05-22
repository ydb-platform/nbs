#include "factory.h"

#include "alter_placement_group_membership.h"
#include "alter_volume.h"
#include "assign_volume.h"
#include "backup_volume.h"
#include "cancel_endpoint_in_flight_requests.h"
#include "check_range.h"
#include "create_checkpoint.h"
#include "create_placement_group.h"
#include "create_volume.h"
#include "create_volume_from_device.h"
#include "create_volume_link.h"
#include "delete_checkpoint.h"
#include "describe_disk_registry_config.h"
#include "describe_endpoint.h"
#include "describe_placement_group.h"
#include "describe_volume.h"
#include "describe_volume_model.h"
#include "destroy_placement_group.h"
#include "destroy_volume.h"
#include "destroy_volume_link.h"
#include "discover_instances.h"
#include "endpoint_proxy.h"
#include "execute_action.h"
#include "get_changed_blocks.h"
#include "get_checkpoint_status.h"
#include "kick_endpoint.h"
#include "list_endpoints.h"
#include "list_keyrings.h"
#include "list_placement_groups.h"
#include "list_volumes.h"
#include "ping.h"
#include "query_agents_info.h"
#include "query_available_storage.h"
#include "read_blocks.h"
#include "refresh_endpoint.h"
#include "resize_volume.h"
#include "resume_device.h"
#include "start_endpoint.h"
#include "stat_volume.h"
#include "stop_endpoint.h"
#include "update_disk_registry_config.h"
#include "write_blocks.h"
#include "zero_blocks.h"

#include <util/generic/algorithm.h>
#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THandlerFactory
{
    using TFactoryFunc = std::function<TCommandPtr(IBlockStorePtr)>;

    THashMap<TString, TFactoryFunc> HandlerFactoryFuncs = {
        { "alterplacementgroupmembership", NewAlterPlacementGroupMembershipCommand },
        { "altervolume", NewAlterVolumeCommand },
        { "assignvolume", NewAssignVolumeCommand },
        { "backupvolume", NewBackupVolumeCommand },
        { "checkrange", NewCheckRangeCommand },
        { "createcheckpoint", NewCreateCheckpointCommand },
        { "createplacementgroup", NewCreatePlacementGroupCommand },
        { "createvolume", NewCreateVolumeCommand },
        { "createvolumefromdevice", NewCreateVolumeFromDeviceCommand },
        { "createvolumelink", NewCreateVolumeLinkCommand },
        { "deletecheckpoint", NewDeleteCheckpointCommand },
        { "getcheckpointstatus", NewGetCheckpointStatusCommand },
        { "describediskregistryconfig", NewDescribeDiskRegistryConfigCommand },
        { "describeendpoint", NewDescribeEndpointCommand },
        { "describeplacementgroup", NewDescribePlacementGroupCommand },
        { "describevolume", NewDescribeVolumeCommand },
        { "describevolumemodel", NewDescribeVolumeModelCommand },
        { "destroyplacementgroup", NewDestroyPlacementGroupCommand },
        { "destroyvolume", NewDestroyVolumeCommand },
        { "destroyvolumelink", NewDestroyVolumeLinkCommand },
        { "discoverinstances", NewDiscoverInstancesCommand },
        { "executeaction", NewExecuteActionCommand },
        { "getchangedblocks", NewGetChangedBlocksCommand },
        { "kickendpoint", NewKickEndpointCommand },
        { "listendpoints", NewListEndpointsCommand },
        { "listkeyrings", NewListKeyringsCommand },
        { "listplacementgroups", NewListPlacementGroupsCommand },
        { "listvolumes", NewListVolumesCommand },
        { "ping", NewPingCommand },
        { "queryagentsinfo", NewQueryAgentsInfoCommand },
        { "queryavailablestorage", NewQueryAvailableStorageCommand },
        { "readblocks", NewReadBlocksCommand },
        { "refreshendpoint", NewRefreshEndpointCommand },
        { "cancelendpointinflightrequests", NewCancelEndpointInFlightRequestsCommand },
        { "resizevolume", NewResizeVolumeCommand },
        { "restorevolume", NewRestoreVolumeCommand },
        { "resumedevice", NewResumeDeviceCommand },
        { "startendpoint", NewStartEndpointCommand },
        { "statvolume", NewStatVolumeCommand },
        { "stopendpoint", NewStopEndpointCommand },
        { "updatediskregistryconfig", NewUpdateDiskRegistryConfigCommand },
        { "writeblocks", NewWriteBlocksCommand },
        { "zeroblocks", NewZeroBlocksCommand },
        { "startproxyendpoint", NewStartProxyEndpointCommand },
        { "stopproxyendpoint", NewStopProxyEndpointCommand },
        { "listproxyendpoints", NewListProxyEndpointsCommand },
    };
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr GetHandler(
    const TString& name,
    IBlockStorePtr client)
{
    auto& factoryFuncs = Singleton<THandlerFactory>()->HandlerFactoryFuncs;
    auto* func = factoryFuncs.FindPtr(name);
    if (func) {
        return (*func)(std::move(client));
    }

    return nullptr;
}

TVector<TString> GetHandlerNames()
{
    const auto& factoryFuncs = Singleton<THandlerFactory>()->HandlerFactoryFuncs;

    TVector<TString> names;
    names.reserve(factoryFuncs.size());
    for (const auto& pair: factoryFuncs) {
        names.push_back(pair.first);
    }

    Sort(names);
    return names;
}

}   // namespace NCloud::NBlockStore::NClient
