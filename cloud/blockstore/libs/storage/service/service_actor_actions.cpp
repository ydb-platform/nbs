#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleExecuteAction(
    const TEvService::TEvExecuteActionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& request = msg->Record;

    TString action = request.GetAction();
    action.to_lower();

    auto& input = *request.MutableInput();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LWTRACK(
        RequestReceived_Service,
        msg->CallContext->LWOrbit,
        "ExecuteAction_" + action,
        msg->CallContext->RequestId);

    using TFunc =
        TResultOrError<NActors::IActorPtr>(TServiceActor::*)(TRequestInfoPtr, TString);
    const THashMap<TString, TFunc> actions = {
        {"diskregistrysetwritablestate",      &TServiceActor::CreateWritableStateActionActor               },
        {"backupdiskregistrystate",           &TServiceActor::CreateBackupDiskRegistryStateActor           },
        {"checkblob",                         &TServiceActor::CreateCheckBlobActionActor                   },
        {"checkrange",                        &TServiceActor::CreateCheckRangeActionActor                  },
        {"compactrange",                      &TServiceActor::CreateCompactRangeActionActor                },
        {"configurevolumebalancer",           &TServiceActor::CreateConfigureVolumeBalancerActionActor     },
        {"deletecheckpointdata",              &TServiceActor::CreateDeleteCheckpointDataActionActor        },
        {"describeblocks",                    &TServiceActor::CreateDescribeBlocksActionActor              },
        {"describevolume",                    &TServiceActor::CreateDescribeVolumeActionActor              },
        {"diskregistrychangestate",           &TServiceActor::CreateDiskRegistryChangeStateActor           },
        {"drainnode",                         &TServiceActor::CreateDrainNodeActionActor                   },
        {"finishfilldisk",                    &TServiceActor::CreateFinishFillDiskActionActor              },
        {"getcompactionstatus",               &TServiceActor::CreateGetCompactionStatusActionActor         },
        {"getdependentdisks",                 &TServiceActor::CreateGetDependentDisksActionActor           },
        {"getpartitioninfo",                  &TServiceActor::CreateGetPartitionInfoActionActor            },
        {"getrebuildmetadatastatus",          &TServiceActor::CreateRebuildMetadataStatusActionActor       },
        {"getscandiskstatus",                 &TServiceActor::CreateScanDiskStatusActionActor              },
        {"killtablet",                        &TServiceActor::CreateKillTabletActionActor                  },
        {"migrationdiskregistrydevice",       &TServiceActor::CreateMigrationDiskRegistryDeviceActor       },
        {"modifytags",                        &TServiceActor::CreateModifyTagsActionActor                  },
        {"reallocatedisk",                    &TServiceActor::CreateReallocateDiskActionActor              },
        {"reassigndiskregistry",              &TServiceActor::CreateReassignDiskRegistryActionActor        },
        {"rebasevolume",                      &TServiceActor::CreateRebaseVolumeActionActor                },
        {"rebindvolumes",                     &TServiceActor::CreateRebindVolumesActionActor               },
        {"rebuildmetadata",                   &TServiceActor::CreateRebuildMetadataActionActor             },
        {"replacedevice",                     &TServiceActor::CreateReplaceDeviceActionActor               },
        {"resettablet",                       &TServiceActor::CreateResetTabletActionActor                 },
        {"restorediskregistrystate",          &TServiceActor::CreateRestoreDiskRegistryStateActor          },
        {"scandisk",                          &TServiceActor::CreateScanDiskActionActor                    },
        {"setuserid",                         &TServiceActor::CreateSetUserIdActionActor                   },
        {"suspenddevice",                     &TServiceActor::CreateSuspendDeviceActionActor               },
        {"updatediskblocksize",               &TServiceActor::CreateUpdateDiskBlockSizeActionActor         },
        {"updatediskreplicacount",            &TServiceActor::CreateUpdateDiskReplicaCountActionActor      },
        {"updateplacementgroupsettings",      &TServiceActor::CreateUpdatePlacementGroupSettingsActor      },
        {"updatevolumeparams",                &TServiceActor::CreateUpdateVolumeParamsActor                },
        {"updateusedblocks",                  &TServiceActor::CreateUpdateUsedBlocksActionActor            },
        {"getdiskregistrytabletinfo",         &TServiceActor::CreateGetDiskRegistryTabletInfo              },
        {"creatediskfromdevices",             &TServiceActor::CreateCreateDiskFromDevicesActionActor       },
        {"changediskdevice",                  &TServiceActor::CreateChangeDiskDeviceActionActor            },
        {"setupchannels",                     &TServiceActor::CreateSetupChannelsActionActor               },
        {"updatediskregistryagentlistparams", &TServiceActor::CreateUpdateDiskRegistryAgentListParamsActor },
        {"changestorageconfig",               &TServiceActor::CreateChangeStorageConfigActionActor         },
        {"getnameservernodes",                &TServiceActor::CreateGetNameserverNodesActionActor          },
        {"cms",                               &TServiceActor::CreateCmsActionActor                         },
        {"flushprofilelog",                   &TServiceActor::CreateFlushProfileLogActor                   },
        {"getdiskagentnodeid",                &TServiceActor::CreateGetDiskAgentNodeIdActor                },
        {"waitdependentdiskstoswitchnode",    &TServiceActor::CreateWaitDependentDisksToSwitchNodeActor    },
        {"partiallysuspenddiskagent",         &TServiceActor::CreatePartiallySuspendDiskAgentActor         },
        {"getstorageconfig",                  &TServiceActor::CreateGetStorageConfigActor                  },
        {"backuppathdescriptions",            &TServiceActor::CreateBackupPathDescriptionsActor            },
        {"backuptabletbootinfos",             &TServiceActor::CreateBackupTabletBootInfosActor             },
        {"getcapacity",                       &TServiceActor::CreateGetCapacityActor                       },
    };

    NProto::TError error;

    auto it = actions.find(action);
    if (it == actions.end()) {
        error = MakeError(E_ARGUMENT, "No suitable action found");
    } else {
        auto actionFunc = it->second;
        auto result = std::invoke(
            actionFunc,
            this,
            requestInfo,
            std::move(input));
        if (auto actor = result.ExtractResult(); actor) {
            NCloud::Register(ctx, std::move(actor));
            return;
        }
        error = result.GetError();
    }

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));

    LWTRACK(
        ResponseSent_Service,
        msg->CallContext->LWOrbit,
        "ExecuteAction_" + action,
        msg->CallContext->RequestId);

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
