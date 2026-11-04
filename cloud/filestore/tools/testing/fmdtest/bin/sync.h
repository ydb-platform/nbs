#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/json_value.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/socket.h>

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

//
// The instances of the multi-client test are synchronized via a hub: each
// instance connects to the hub and, at each barrier, sends its payload and
// receives the payloads of all the instances. The hub replies only after all
// the instances have reached the same barrier.
//
// The messages are JSON objects framed by a 4-byte little-endian length.
//

class TSyncClient
{
private:
    TSocketHolder Socket;
    const TDuration Timeout;

public:
    TSyncClient(TSocketHolder socket, TDuration timeout);

    /**
     * Connects to the hub and introduces the instance.
     *
     * @param host - Hub host.
     * @param port - Hub port.
     * @param instanceId - Id of this instance.
     * @param timeout - Max wait for each network operation, including the
     *  wait for the other instances at a barrier.
     * @return - The client or an error.
     */
    static TResultOrError<std::unique_ptr<TSyncClient>> Connect(
        const TString& host,
        ui16 port,
        ui32 instanceId,
        TDuration timeout);

    /**
     * Waits until all the instances reach the barrier.
     *
     * @param round - Round number.
     * @param phase - Phase name within the round.
     * @param payload - Payload of this instance.
     * @return - The payloads of all the instances indexed by instance id.
     */
    TResultOrError<TVector<NJson::TJsonValue>> Exchange(
        ui32 round,
        const TString& phase,
        const NJson::TJsonValue& payload);

    /**
     * Tells the hub that this instance has finished.
     *
     * @return - Error if the hub is not reachable.
     */
    NProto::TError Finish();
};

////////////////////////////////////////////////////////////////////////////////

/**
 * Runs the hub until all the instances finish or any of them fails.
 *
 * @param Log - Log.
 * @param port - Port to listen on (all addresses).
 * @param instanceCount - Number of the instances.
 * @param timeout - Max wait for the instances at each barrier.
 * @return - Process exit code.
 */
int RunSyncHub(TLog& Log, ui16 port, ui32 instanceCount, TDuration timeout);

}   // namespace NCloud::NFileStore
