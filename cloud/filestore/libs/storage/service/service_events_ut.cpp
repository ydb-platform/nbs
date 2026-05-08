#include "service_private.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// These tests verify that public (TEvService) and private (TEvServicePrivate,
// TEvIndexTablet) event IDs do not accidentally change. Events transferred
// by wire use TProtoRequestEvent / TProtoResponseEvent (backed by TEventPB);
// their IDs must be stable across releases for compatibility. Local events
// (TRequestEvent / TResponseEvent) also require stable IDs so that the actor
// dispatcher continues to route them correctly.

Y_UNIT_TEST_SUITE(TEventIds)
{
    Y_UNIT_TEST(PublicServiceEvents)
    {
        // TEvService – public API wire events (TProtoRequestEvent /
        // TProtoResponseEvent).
        UNIT_ASSERT_VALUES_EQUAL(275054695u, TEvService::EvPingRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054696u, TEvService::EvPingResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054697u, TEvService::EvCreateFileStoreRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054698u, TEvService::EvCreateFileStoreResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054699u, TEvService::EvDestroyFileStoreRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054700u, TEvService::EvDestroyFileStoreResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054701u, TEvService::EvGetFileStoreInfoRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054702u, TEvService::EvGetFileStoreInfoResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054703u, TEvService::EvCreateSessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054704u, TEvService::EvCreateSessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054705u, TEvService::EvDestroySessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054706u, TEvService::EvDestroySessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054707u, TEvService::EvPingSessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054708u, TEvService::EvPingSessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054709u, TEvService::EvCreateCheckpointRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054710u, TEvService::EvCreateCheckpointResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054711u, TEvService::EvDestroyCheckpointRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054712u, TEvService::EvDestroyCheckpointResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054713u, TEvService::EvCreateNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054714u, TEvService::EvCreateNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054715u, TEvService::EvUnlinkNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054716u, TEvService::EvUnlinkNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054717u, TEvService::EvRenameNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054718u, TEvService::EvRenameNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054719u, TEvService::EvAccessNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054720u, TEvService::EvAccessNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054721u, TEvService::EvListNodesRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054722u, TEvService::EvListNodesResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054723u, TEvService::EvSetNodeAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054724u, TEvService::EvSetNodeAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054725u, TEvService::EvGetNodeAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054726u, TEvService::EvGetNodeAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054727u, TEvService::EvSetNodeXAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054728u, TEvService::EvSetNodeXAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054729u, TEvService::EvGetNodeXAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054730u, TEvService::EvGetNodeXAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054731u, TEvService::EvListNodeXAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054732u, TEvService::EvListNodeXAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054733u, TEvService::EvRemoveNodeXAttrRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054734u, TEvService::EvRemoveNodeXAttrResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054735u, TEvService::EvCreateHandleRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054736u, TEvService::EvCreateHandleResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054737u, TEvService::EvDestroyHandleRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054738u, TEvService::EvDestroyHandleResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054739u, TEvService::EvAcquireLockRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054740u, TEvService::EvAcquireLockResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054741u, TEvService::EvReleaseLockRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054742u, TEvService::EvReleaseLockResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054743u, TEvService::EvTestLockRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054744u, TEvService::EvTestLockResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054745u, TEvService::EvReadDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054746u, TEvService::EvReadDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054747u, TEvService::EvWriteDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054748u, TEvService::EvWriteDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054749u, TEvService::EvAllocateDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054750u, TEvService::EvAllocateDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054751u, TEvService::EvResolvePathRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054752u, TEvService::EvResolvePathResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054753u, TEvService::EvReadLinkRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054754u, TEvService::EvReadLinkResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054755u, TEvService::EvAlterFileStoreRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054756u, TEvService::EvAlterFileStoreResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054757u, TEvService::EvResizeFileStoreRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054758u, TEvService::EvResizeFileStoreResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054759u, TEvService::EvDescribeFileStoreModelRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054760u, TEvService::EvDescribeFileStoreModelResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054761u, TEvService::EvSubscribeSessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054762u, TEvService::EvSubscribeSessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054763u, TEvService::EvGetSessionEventsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054764u, TEvService::EvGetSessionEventsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054765u, TEvService::EvRegisterLocalFileStore);
        UNIT_ASSERT_VALUES_EQUAL(275054766u, TEvService::EvUnregisterLocalFileStore);

        UNIT_ASSERT_VALUES_EQUAL(275054769u, TEvService::EvAddClusterNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054770u, TEvService::EvAddClusterNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054771u, TEvService::EvRemoveClusterNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054772u, TEvService::EvRemoveClusterNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054773u, TEvService::EvListClusterNodesRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054774u, TEvService::EvListClusterNodesResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054775u, TEvService::EvAddClusterClientsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054776u, TEvService::EvAddClusterClientsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054777u, TEvService::EvRemoveClusterClientsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054778u, TEvService::EvRemoveClusterClientsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054779u, TEvService::EvListClusterClientsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054780u, TEvService::EvListClusterClientsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054781u, TEvService::EvUpdateClusterRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054782u, TEvService::EvUpdateClusterResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054783u, TEvService::EvStatFileStoreRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054784u, TEvService::EvStatFileStoreResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054785u, TEvService::EvResetSessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054786u, TEvService::EvResetSessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054787u, TEvService::EvListFileStoresRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054788u, TEvService::EvListFileStoresResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054789u, TEvService::EvExecuteActionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054790u, TEvService::EvExecuteActionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275054791u, TEvService::EvToggleServiceStateRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054792u, TEvService::EvToggleServiceStateResponse);
    }

    Y_UNIT_TEST(PrivateServiceEvents)
    {
        // TEvServicePrivate – private service-worker events (TRequestEvent /
        // TResponseEvent backed by TEventLocal).
        UNIT_ASSERT_VALUES_EQUAL(
            275120332u,
            TEvServicePrivate::EvPingSession);
        UNIT_ASSERT_VALUES_EQUAL(
            275120333u,
            TEvServicePrivate::EvCreateSession);
        UNIT_ASSERT_VALUES_EQUAL(
            275120334u,
            TEvServicePrivate::EvSessionCreated);
        UNIT_ASSERT_VALUES_EQUAL(
            275120335u,
            TEvServicePrivate::EvSessionDestroyed);
        UNIT_ASSERT_VALUES_EQUAL(
            275120336u,
            TEvServicePrivate::EvUpdateStats);
    }

    Y_UNIT_TEST(TabletEvents)
    {
        // TEvIndexTablet – private tablet wire events (TProtoRequestEvent /
        // TProtoResponseEvent).
        UNIT_ASSERT_VALUES_EQUAL(275054998u, TEvIndexTablet::EvWaitReadyRequest);
        UNIT_ASSERT_VALUES_EQUAL(275054999u, TEvIndexTablet::EvWaitReadyResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055000u, TEvIndexTablet::EvCreateSessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055001u, TEvIndexTablet::EvCreateSessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055002u, TEvIndexTablet::EvDestroySessionRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055003u, TEvIndexTablet::EvDestroySessionResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055004u, TEvIndexTablet::EvGetStorageStatsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055005u, TEvIndexTablet::EvGetStorageStatsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055006u, TEvIndexTablet::EvGetFileSystemConfigRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055007u, TEvIndexTablet::EvGetFileSystemConfigResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055008u, TEvIndexTablet::EvGetStorageConfigFieldsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055009u, TEvIndexTablet::EvGetStorageConfigFieldsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055010u, TEvIndexTablet::EvChangeStorageConfigRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055011u, TEvIndexTablet::EvChangeStorageConfigResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055012u, TEvIndexTablet::EvDescribeDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055013u, TEvIndexTablet::EvDescribeDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055014u, TEvIndexTablet::EvDescribeSessionsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055015u, TEvIndexTablet::EvDescribeSessionsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055016u, TEvIndexTablet::EvGenerateBlobIdsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055017u, TEvIndexTablet::EvGenerateBlobIdsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055018u, TEvIndexTablet::EvAddDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055019u, TEvIndexTablet::EvAddDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055020u, TEvIndexTablet::EvForcedOperationRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055021u, TEvIndexTablet::EvForcedOperationResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055022u, TEvIndexTablet::EvConfigureShardsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055023u, TEvIndexTablet::EvConfigureShardsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055024u, TEvIndexTablet::EvConfigureAsShardRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055025u, TEvIndexTablet::EvConfigureAsShardResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055026u, TEvIndexTablet::EvGetStorageConfigRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055027u, TEvIndexTablet::EvGetStorageConfigResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055028u, TEvIndexTablet::EvGetNodeAttrBatchRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055029u, TEvIndexTablet::EvGetNodeAttrBatchResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055030u, TEvIndexTablet::EvWriteCompactionMapRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055031u, TEvIndexTablet::EvWriteCompactionMapResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055032u, TEvIndexTablet::EvUnsafeDeleteNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055033u, TEvIndexTablet::EvUnsafeDeleteNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055034u, TEvIndexTablet::EvUnsafeUpdateNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055035u, TEvIndexTablet::EvUnsafeUpdateNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055036u, TEvIndexTablet::EvUnsafeGetNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055037u, TEvIndexTablet::EvUnsafeGetNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055038u, TEvIndexTablet::EvForcedOperationStatusRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055039u, TEvIndexTablet::EvForcedOperationStatusResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055040u, TEvIndexTablet::EvGetFileSystemTopologyRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055041u, TEvIndexTablet::EvGetFileSystemTopologyResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055042u, TEvIndexTablet::EvRestartTabletRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055043u, TEvIndexTablet::EvRestartTabletResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055044u, TEvIndexTablet::EvRenameNodeInDestinationRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055045u, TEvIndexTablet::EvRenameNodeInDestinationResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055046u, TEvIndexTablet::EvReadNodeRefsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055047u, TEvIndexTablet::EvReadNodeRefsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055048u, TEvIndexTablet::EvSetHasXAttrsRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055049u, TEvIndexTablet::EvSetHasXAttrsResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055050u, TEvIndexTablet::EvMarkNodeRefsExhaustiveRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055051u, TEvIndexTablet::EvMarkNodeRefsExhaustiveResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055052u, TEvIndexTablet::EvUnsafeCreateNodeRefRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055053u, TEvIndexTablet::EvUnsafeCreateNodeRefResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055054u, TEvIndexTablet::EvUnsafeDeleteNodeRefRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055055u, TEvIndexTablet::EvUnsafeDeleteNodeRefResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055056u, TEvIndexTablet::EvUnsafeUpdateNodeRefRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055057u, TEvIndexTablet::EvUnsafeUpdateNodeRefResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055058u, TEvIndexTablet::EvUnsafeGetNodeRefRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055059u, TEvIndexTablet::EvUnsafeGetNodeRefResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055060u, TEvIndexTablet::EvUnsafeCreateHandleRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055061u, TEvIndexTablet::EvUnsafeCreateHandleResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055062u, TEvIndexTablet::EvPrepareUnlinkDirectoryNodeInShardRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055063u, TEvIndexTablet::EvPrepareUnlinkDirectoryNodeInShardResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055064u, TEvIndexTablet::EvAbortUnlinkDirectoryNodeInShardRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055065u, TEvIndexTablet::EvAbortUnlinkDirectoryNodeInShardResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055066u, TEvIndexTablet::EvUnsafeCreateNodeRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055067u, TEvIndexTablet::EvUnsafeCreateNodeResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055068u, TEvIndexTablet::EvDeleteResponseLogEntryRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055069u, TEvIndexTablet::EvDeleteResponseLogEntryResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055070u, TEvIndexTablet::EvGetResponseLogEntryRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055071u, TEvIndexTablet::EvGetResponseLogEntryResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055072u, TEvIndexTablet::EvWriteResponseLogEntryRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055073u, TEvIndexTablet::EvWriteResponseLogEntryResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055074u, TEvIndexTablet::EvConfirmAddDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055075u, TEvIndexTablet::EvConfirmAddDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055076u, TEvIndexTablet::EvCancelAddDataRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055077u, TEvIndexTablet::EvCancelAddDataResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055078u, TEvIndexTablet::EvUnsafeChangeTabletStateRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055079u, TEvIndexTablet::EvUnsafeChangeTabletStateResponse);

        UNIT_ASSERT_VALUES_EQUAL(275055080u, TEvIndexTablet::EvListNodesInternalRequest);
        UNIT_ASSERT_VALUES_EQUAL(275055081u, TEvIndexTablet::EvListNodesInternalResponse);
    }
}

}   // namespace NCloud::NFileStore::NStorage
