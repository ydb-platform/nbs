#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns an IStorageNode that forwards every call over a single TCP
 * connection to an sn server at `host:port`. Each call is wrapped in a
 * TDeviceProtocolRequest, sent length-prefixed, and its
 * TDeviceProtocolResponse is unpacked into the matching concrete
 * response.
 *
 * Concurrency: methods on the returned client may be called from any
 * silk fiber; concurrent calls are serialized on a single shared
 * connection.
 *
 * Errors:
 *   - I/O failures (connect / send / recv / parse) surface as a
 *     response whose Error field is set to E_REJECTED. The connection
 *     is closed and reopened on the next call.
 *   - A TDeviceProtocolResponse.ProtocolError from the server is
 *     copied into the concrete response's Error field.
 *
 * Must be called from a silk fiber context.
 *
 * @param host - Server hostname or IP.
 * @param port - Server TCP port.
 *
 * @return - Shared owner of the client instance.
 */
IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
