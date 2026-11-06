#pragma once

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/public/api/protos/actions.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// These events are local: the proxy and its callers belong to the same node.
struct TEvCellProxy
{
    struct TInitializeRequest
    {
        NCells::ICellManagerPtr CellManager;
        NClient::TClientAppConfigPtr ClientConfig;

        TInitializeRequest(NCells::ICellManagerPtr cellManager,
                           NClient::TClientAppConfigPtr clientConfig)
            : CellManager(std::move(cellManager))
            , ClientConfig(std::move(clientConfig))
        {}
    };

    struct TDescribeVolumeRequest
    {
        TString CellId;
        TString DiskId;
        TDuration Timeout;

        TDescribeVolumeRequest(TString cellId, TString diskId,
                               TDuration timeout = TDuration::Seconds(30))
            : CellId(std::move(cellId))
            , DiskId(std::move(diskId))
            , Timeout(timeout)
        {}
    };

    struct TDescribeVolumeResponse
    {
        NProto::TVolume Volume;

        TDescribeVolumeResponse() = default;

        explicit TDescribeVolumeResponse(NProto::TVolume volume)
            : Volume(std::move(volume))
        {}
    };

    struct TExecuteActionRequest
    {
        TString CellId;
        NProto::TExecuteActionRequest Record;
        TDuration Timeout;

        TExecuteActionRequest(TString cellId,
                              NProto::TExecuteActionRequest record,
                              TDuration timeout = TDuration::Seconds(30))
            : CellId(std::move(cellId))
            , Record(std::move(record))
            , Timeout(timeout)
        {}
    };

    struct TExecuteActionResponse
    {
        TString Output;

        TExecuteActionResponse() = default;

        explicit TExecuteActionResponse(TString output)
            : Output(std::move(output))
        {}
    };

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::CELLS_START,
        EvInitializeRequest = EvBegin + 1,
        EvInitializeResponse,
        EvDescribeVolumeRequest,
        EvDescribeVolumeResponse,
        EvExecuteActionRequest,
        EvExecuteActionResponse,
        EvEnd
    };

    static_assert(EvEnd < static_cast<int>(TBlockStoreEvents::CELLS_END));

    using TEvInitializeRequest =
        TRequestEvent<TInitializeRequest, EvInitializeRequest>;
    using TEvInitializeResponse = TResponseEvent<TEmpty, EvInitializeResponse>;
    using TEvDescribeVolumeRequest =
        TRequestEvent<TDescribeVolumeRequest, EvDescribeVolumeRequest>;
    using TEvDescribeVolumeResponse =
        TResponseEvent<TDescribeVolumeResponse, EvDescribeVolumeResponse>;
    using TEvExecuteActionRequest =
        TRequestEvent<TExecuteActionRequest, EvExecuteActionRequest>;
    using TEvExecuteActionResponse =
        TResponseEvent<TExecuteActionResponse, EvExecuteActionResponse>;
};

NActors::TActorId MakeCellProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
