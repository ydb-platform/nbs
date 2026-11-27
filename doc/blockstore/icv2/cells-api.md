# Using IC V2 from Cells

Cells receives an `IDataEndpointFactory` at startup. It uses the factory to create an endpoint for a remote host, then sends block I/O through the returned `IBlockStore`. [Cells endpoint setup](../../../cloud/blockstore/libs/cells/impl/endpoint_bootstrap_impl.cpp).

## Interfaces and base types

| Interface | Extends | Purpose in Cells |
| --- | --- | --- |
| [`IDataEndpointFactory`](../../../cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h) | None | Creates an endpoint with `CreateDataEndpoint()` and provides a status snapshot through `GetDataEndpointStatus()`. |
| [`IBlockStore`](../../../cloud/blockstore/libs/service/service.h) | [`IStartable`](../../../cloud/storage/core/libs/common/startable.h) | Sends the supported block I/O requests through the endpoint returned by the factory. |
| [`IClientEndpointHandler`](../../../cloud/storage/core/libs/common/endpoint_handler.h) | None | Implements this optional callback interface to track endpoint readiness. |

NBS owns the service lifecycle. Its `IService` extends both `IStartable` and `IDataEndpointFactory`. NBS starts the service before Cells creates endpoints and exposes the factory interface to Cells. [Service interface](../../../cloud/blockstore/libs/icv2/service/service.h), [NBS startup](../../../cloud/blockstore/libs/daemon/common/bootstrap.cpp).

## Create an endpoint

Call `CreateDataEndpoint(cellId, host, handler)` with the remote cell ID and the hostname selected by Cells. The factory uses the port configured for that cell. Check the returned `TResultOrError<IBlockStorePtr>` for a creation error. The factory returns the endpoint while connection attempts run asynchronously. `HandleConnected()` signals readiness for I/O. [Factory contract](../../../cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h), [port selection](../../../cloud/blockstore/libs/icv2/service/service.cpp).

Pass a handler if Cells needs to react to connection changes. Initialize it before creating the endpoint because it may receive a callback before the factory call returns. Use weak references back to the endpoint or its owner. Keep callbacks nonblocking and non-throwing. [Callback contract](../../../cloud/storage/core/libs/common/endpoint_handler.h), [Cells transport switcher](../../../cloud/blockstore/libs/cells/impl/transport_switcher.cpp).

| Callback | Meaning |
| --- | --- |
| `HandleConnected()` | The endpoint is ready for I/O. Also called after a successful reconnect. |
| `HandleDisconnected()` | An established connection has dropped. |
| `HandleUnavailable()` | A connection attempt failed. This can repeat across attempts. |

For a status snapshot, pass the original endpoint to `GetDataEndpointStatus()` on the same factory. The returned `TPeerStatus` contains the current state (`Idle`, `Connecting`, `Connected`, or `Disconnected`) and counts of connection attempts and failures. [Status type](../../../cloud/storage/core/libs/icv2/peer/iface/status.h).

For disks served through the same remote NBS host, you can share one `IBlockStore` endpoint or create several endpoints to that host. Each call to `CreateDataEndpoint()` creates an independent endpoint with its own IC V2 channel once connected. Each request carries the `DiskId` and `SessionId` for the disk it targets. [Endpoint creation](../../../cloud/blockstore/libs/icv2/service/host_endpoint.cpp), [request metadata](../../../cloud/blockstore/libs/service/request.cpp).

## Supported requests

- `ReadBlocks()`
- `ReadBlocksLocal()`
- `WriteBlocks()`
- `WriteBlocksLocal()`
- `ZeroBlocks()`

## Release an endpoint

Release the endpoint when Cells no longer routes I/O to it. The service finishes cleanup asynchronously. If the caller needs to wait for cleanup, call `endpoint->Stop()` outside notification and result callbacks. [Endpoint lifecycle](../../../cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h).
