# NBS cells: architecture, data path, and configuration notes

This document records the current understanding of `cloud/blockstore/libs/cells`
and the surrounding endpoint, session, vhost, and Volume Tablet code. It is
intended both as engineering documentation and as a context handoff for future
investigation sessions.

The document describes the code as it exists now. It is not a production rollout
runbook: concrete ports, certificates, host lists, and safe rollout order still
have to be checked against the target environment.

## Short version

`cells` makes an NBS server capable of serving an endpoint for a disk whose
volume belongs to another cell.

At `StartEndpoint` time the server:

1. sends `DescribeVolume` to its local service and to available configured peer
   cells;
2. takes the first valid successful response and remembers its `cellId`;
3. creates an ordinary client session, but binds its control and data backends
   either to the local service or to an NBS host in the discovered peer cell;
4. passes the resulting `ISession` to vhost/NBD/etc.;
5. does not repeat cell discovery for every I/O request.

The upper data path is therefore the same for local and foreign cells. A foreign
cell adds an explicit NBS-server-to-NBS-server gRPC/RDMA hop before the request
enters the ordinary storage service of the owning cell.

```text
local cell:
    VM -> vhost -> ISession -> TStorageDataClient
       -> local IStorage/NBS Service -> Volume Tablet -> partitions/devices

foreign cell:
    VM -> vhost -> ISession -> TStorageDataClient
       -> TRemoteStorage -> gRPC/RDMA -> NBS Service in the owning cell
       -> Volume Tablet -> partitions/devices
```

## Terminology and independent routing dimensions

Several uses of the words `local` and `remote` coexist. They must not be mixed.

| Term | Meaning |
|---|---|
| local cell | The volume was found through the NBS service handling the endpoint; the returned `cellId` is empty. |
| foreign/remote cell | The volume was found through a configured peer cell; the returned `cellId` is non-empty. |
| `VOLUME_MOUNT_LOCAL` | The Volume Tablet is external-booted inside the NBS server handling that mount. This says nothing about where disk data physically resides. |
| `VOLUME_MOUNT_REMOTE` | The NBS service uses a tablet pipe to a Hive-managed Volume Tablet, possibly on another host in the same cell. |
| `ReadBlocksLocal` / `WriteBlocksLocal` | The request contains process-local buffers/`sglist`. It does not mean that the volume, tablet, or devices are local. |
| `TReadBlocksRemoteRequestActor` | Converts a local-buffer request into a serializable request before a remote Volume Tablet hop. It is not the cells routing component. |

A disk is addressed by `DiskId`. `DescribeVolume` returns a `TVolume` describing
that logical disk. The Volume Tablet is the tablet coordinating the volume; its
current process/node placement is separate from the cell in which the volume is
registered. Partitions and physical devices add further placement and network
hops.

Consequently there are two independent decisions:

```text
1. Which cell owns the volume?
   -> local service or cells gRPC/RDMA endpoint

2. Where is the owning cell's Volume Tablet running?
   -> direct local actor or tablet pipe/YDB interconnect
```

## Main components

- [`iface/cell_manager.h`](iface/cell_manager.h) defines `ICellManager`:
  cross-cell `DescribeVolume` and lookup of an endpoint by `cellId`.
- [`impl/cell_manager_impl.cpp`](impl/cell_manager_impl.cpp) owns configured
  `TCell` objects and collects endpoints for discovery.
- [`impl/cell_impl.cpp`](impl/cell_impl.cpp) maintains active hosts for one cell,
  selects discovery hosts, and randomly selects a host for a new endpoint.
- [`impl/cell_host_impl.cpp`](impl/cell_host_impl.cpp) builds gRPC and optional
  RDMA endpoints for one configured NBS host.
- [`impl/describe_volume.cpp`](impl/describe_volume.cpp) implements parallel
  local and multi-cell discovery.
- [`impl/remote_storage.cpp`](impl/remote_storage.cpp) adapts a remote
  `IBlockStore` endpoint to the `IStorage` interface used by the endpoint data
  path.
- [`../endpoints/session_manager.cpp`](../endpoints/session_manager.cpp) is the
  integration point: it runs discovery, chooses local or cell storage, builds
  the client/session stack, mounts the volume, and returns the session to the
  endpoint manager.

## `StartEndpoint` and volume discovery

The important path begins in
[`TSessionManager::CreateSessionImpl`](../endpoints/session_manager.cpp):

```text
StartEndpoint
  -> EndpointManager
  -> TSessionManager::CreateSessionImpl
  -> CellManager::DescribeVolume
  -> CreateEndpoint(volume, cellId)
  -> endpoint->Start()
  -> Session::MountVolume
  -> pass ISession to the endpoint listener/vhost
```

`CreateSessionImpl` first calls `DescribeVolume`, extracts both `volume` and
`cellId`, then creates and starts the endpoint. Principal-volume redirects are
followed recursively before the final endpoint is created.

### Discovery fan-out

[`NCells::DescribeVolume`](impl/describe_volume.cpp) builds a list containing:

- up to `DescribeVolumeHostCount` active gRPC clients for every available
  configured peer cell;
- one local entry backed by the current process's `IBlockStore` service.

All selected requests start in parallel. Remote requests carry their expected
`CellId` in the request headers; the local request carries the special label
`local`. The target server checks that a non-empty header `CellId` matches its
configured server `CellId`.

The first valid successful response wins. On success the handler sets:

- an empty response `CellId` for the local service;
- the configured peer `CellId` for a remote response.

During relocation it ignores a successful response whose volume contains the
`source-disk-id` tag, because that response represents the destination copy and
clients must continue using the source/principal disk.

If no request succeeds:

- a timeout produces retriable `E_REJECTED` with `Describe timeout`;
- an unavailable cell or a cell with only retriable failures produces
  `E_REJECTED` with `Not all cells available`;
- complete, reliable absence produces `E_NOT_FOUND` with
  `Volume ... not found in cells`.

This full fan-out happens while starting an endpoint and in operations such as
`GetSession`/session switching that explicitly call `DescribeVolume`. It is not
performed on each read or write.

## Selecting the control and data backends

The cell boundary is implemented in
[`TSessionManager::CreateStorageDataClient`](../endpoints/session_manager.cpp).
The logical branch is:

```cpp
auto service = Service;
IStoragePtr storage;

if (!cellId.empty()) {
    auto endpoint = CellManager->GetCellEndpoint(cellId, clientConfig);
    service = endpoint.GetService();
    storage = endpoint.GetStorage();
} else {
    storage = StorageProvider->CreateStorage(volume, clientId, accessMode);
}

return MakeShared<TStorageDataClient>(storage, service, ...);
```

For a new foreign-cell endpoint, `TCell::PickHost` randomly chooses one active
host in that cell. The address is not returned by `DescribeVolume`; host names,
ports, and transports come from the cells configuration, and the cell manager
has already bootstrapped the corresponding host endpoints.

The chosen `TCellHostEndpoint` contains two interfaces:

- `Service: IBlockStorePtr` for the control plane (`MountVolume`,
  `UnmountVolume`, `ExecuteAction`);
- `Storage: IStoragePtr` for the data plane (`ReadBlocksLocal`,
  `WriteBlocksLocal`, `ZeroBlocks`).

For gRPC transport both ultimately use the gRPC client endpoint. For RDMA,
control and discovery still use gRPC while the data `Storage` wraps the RDMA
endpoint.

## Where `TStorageDataClient` sends I/O

The forwarding methods in
[`TStorageDataClient`](../endpoints/session_manager.cpp) are generated by the
`BLOCKSTORE_IMPLEMENT_METHOD` macro. After macro expansion they are equivalent
to:

```cpp
TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request) override
{
    SetBlockSize(*request);
    PrepareRequestHeaders(*request->MutableHeaders(), *callContext);
    return Storage->ReadBlocksLocal(
        std::move(callContext),
        std::move(request));
}
```

The same macro forwards `WriteBlocksLocal` and `ZeroBlocks` to `Storage`, while
`ExecuteAction` goes to `Service`. Explicit `MountVolume` and `UnmountVolume`
methods also go to `Service`.

The selected `storage` is moved into `TStorageDataClient` once during endpoint
construction. There is no `cellId` condition on every I/O; virtual dispatch
chooses the already bound implementation:

```text
empty cellId     -> local storage provider -> usually TServiceStorage
non-empty cellId -> TCellHostEndpoint::Storage -> TRemoteStorage
```

`TServiceStorage` calls the local process's NBS service. `TRemoteStorage` calls
the selected peer endpoint over the configured cells data transport.

## Full vhost-to-storage call chain

The vhost endpoint listener passes the created `ISession` into
[`NVhost::IServer::StartEndpoint`](../endpoints_vhost/vhost_server.cpp). An
`ISession` is usable as the endpoint's storage interface.

For a read, the main call chain is:

```text
virtio/vhost request
  -> NVhost::TEndpoint::ProcessRequest
  -> IDeviceHandler::Read
  -> ISession::ReadBlocksLocal
  -> TSession::HandleRequest / SendRequest
  -> Client->ReadBlocksLocal
  -> switchable/durable/encryption/throttling/validation wrappers as configured
  -> TStorageDataClient::ReadBlocksLocal
  -> selected Storage->ReadBlocksLocal
```

The analogous write path ends in `Storage->WriteBlocksLocal`, and discard/zero
ends in `Storage->ZeroBlocks`.

## From NBS Service to the Volume Tablet

Once a request has reached the NBS storage service of the owning cell, cells are
no longer involved. The relevant code is
[`TServiceActor::ForwardRequest`](../storage/service/service_actor_forward.cpp).

### Locally external-booted Volume Tablet

If `volume->VolumeActor` is set, the service has the direct actor ID of a Volume
Tablet running in this actor system and forwards the request directly:

```text
ServiceActor -> VolumeActor
```

### Hive-managed/remote Volume Tablet

Otherwise the mounted volume normally has a `VolumeClientActor`. Local-buffer
read/write requests are first converted to serializable protobuf requests by
`TReadBlocksRemoteRequestActor` or `TWriteBlocksRemoteRequestActor`, because an
`sglist` contains process-local memory references.

[`TVolumeClientActor::HandleRequest`](../storage/service/volume_client_actor.cpp)
then:

1. creates an `NTabletPipe` client for the Volume `TabletId`, if necessary;
2. sends the event with `NCloud::PipeSend`;
3. lets the tablet-pipe/Hive/YDB actor machinery resolve the current tablet
   location and use interconnect when it is on another node.

Thus a foreign-cell request can contain two independent network hops:

```text
origin NBS server
  -> cells gRPC/RDMA
  -> selected NBS server in the owning cell
  -> tablet pipe/YDB interconnect, if the Volume Tablet is on another host
  -> Volume Tablet
```

Even a local-cell request can contain the tablet-pipe/interconnect hop. “Local
cell” never guarantees that the Volume Tablet, partitions, or physical devices
are on the endpoint host.

## Mount modes and external boot

Cell routing and `EVolumeMountMode` are separate mechanisms.

- For an effective `VOLUME_MOUNT_LOCAL`, the service locks the Volume Tablet in
  Hive and `TStartVolumeActor` external-boots a local `CreateVolumeTablet(...)`.
  The resulting user actor ID becomes `volume->VolumeActor`, enabling direct
  service-to-volume forwarding.
- For `VOLUME_MOUNT_REMOTE`, the service uses `VolumeClientActor` and a tablet
  pipe to the Hive-managed tablet.
- A requested local mount can be downgraded to remote because of binding or
  preemption. Reasoning about unmount must use the effective mount mode stored
  in client info, not only the requested mode.
- On a successful local `UnmountVolume`, the service explicitly stops the local
  Volume Tablet actor and releases its Hive lock. The logical tablet/volume is
  not deleted; Hive can boot the same tablet in normal remote mode.
- A remote `UnmountVolume` removes the client but does not explicitly stop the
  Hive-managed Volume Tablet.
- A guest filesystem `umount` is not necessarily an NBS `UnmountVolume`; if the
  endpoint/session remains mounted, NBS tablet state does not change.

On a temporary server, a foreign-cell endpoint is forced to use
`VOLUME_MOUNT_REMOTE`. See `CreateSessionConfig` in
[`session_manager.cpp`](../endpoints/session_manager.cpp).

## Configuration source

The daemon accepts a text-protobuf file through:

```text
--cells-file <path>
```

The file is parsed as `NProto::TCellsConfig`; its schema is
[`cloud/blockstore/config/cells.proto`](../../config/cells.proto). If
`CellsEnabled` is false or absent, the daemon installs the cell-manager stub and
discovery stays local.

The current server's `CellId` is also passed to the gRPC server and is used to
validate `DescribeVolume` requests addressed to a particular cell.

## Configuration example

The following is an illustrative text-protobuf for a server in `cell-a` that
knows how to reach peer `cell-b`. Port and TLS values are placeholders.

```protobuf
CellsEnabled: true
CellId: "cell-a"

# Milliseconds when explicitly set. The code default is 30 seconds.
DescribeVolumeTimeout: 30000

GrpcClientConfig {
    RequestTimeout: 30000
    RetryTimeout: 1000
    # For TLS, SecurePort must also be non-zero so that bootstrap creates
    # a real certificate provider. The endpoint's actual port comes from
    # the peer cell's SecureGrpcPort below.
    # SecurePort: 9767
    # RootCertsFile: "/path/to/ca.pem"
    # CertFile: "/path/to/client.pem"
    # CertPrivateKeyFile: "/path/to/client-key.pem"
}

# Leave zero for pure gRPC. Set a positive worker count for an RDMA data path.
RdmaTransportWorkers: 0

Cells {
    CellId: "cell-b"

    GrpcPort: 9766
    # SecureGrpcPort: 9767
    # RdmaPort: 10020

    Transport: CELL_DATA_TRANSPORT_GRPC
    DescribeVolumeHostCount: 2
    MinCellConnections: 2

    Hosts { Fqdn: "nbs-b-01.example.net" }
    Hosts { Fqdn: "nbs-b-02.example.net" }
    Hosts { Fqdn: "nbs-b-03.example.net" }
}
```

Each server needs its own current `CellId` and a view of the peer cells it must
reach. The implementation always adds the current process's local service to
discovery separately and does not filter the current `CellId` out of the
repeated `Cells` field. Therefore the safe configuration model is to list peer
cells there, not the current cell itself, unless duplicate local/remote discovery
is explicitly intended and understood.

## Configuration fields and defaults

Defaults below come from [`iface/config.cpp`](iface/config.cpp), not only from
protobuf zero values.

### `TCellsConfig`

| Field | Code default | Notes |
|---|---:|---|
| `CellsEnabled` | `false` | Enables real `TCellManager`; otherwise a local-only stub is used. |
| `CellId` | empty | Identity of this NBS server's cell and expected `DescribeVolume` routing label. Must be non-empty in a real cells deployment. |
| `Cells` | empty | Configured peer cells and their NBS hosts. |
| `DescribeVolumeTimeout` | 30 seconds | Explicit proto value is converted from milliseconds. Covers the multi-cell lookup. |
| `GrpcClientConfig` | default `TClientConfig` | Retry, timeout, message size, TLS, authentication, and gRPC client settings. Host and port are supplied by each cell/host. |
| `RdmaTransportWorkers` | `0` | Positive value creates the cells RDMA worker thread pool; zero creates a stub task queue. |

### `TCellConfig`

| Field | Code default | Notes |
|---|---:|---|
| `CellId` | empty | Key returned by discovery and later passed to `GetCellEndpoint`. |
| `GrpcPort` | `0` | Insecure gRPC endpoint used for control/discovery and gRPC data. |
| `SecureGrpcPort` | `0` | If non-zero, preferred over `GrpcPort` and TLS is enabled. |
| `RdmaPort` | `0` | Data endpoint when transport is RDMA. |
| `NbdPort` | `0` | Present in the schema but not supported by current `TCellHost::Start`. |
| `Transport` | `CELL_DATA_TRANSPORT_GRPC` | Default data transport for hosts in this cell. |
| `Hosts` | empty | NBS server FQDNs. Ports are cell-wide; a host can override only `Transport`. |
| `DescribeVolumeHostCount` | `1` | Maximum number of active hosts selected from this cell for one discovery fan-out. |
| `MinCellConnections` | `1` | Minimum number of cell hosts activated and kept in the active pool. Must be sized consistently with discovery count and availability goals. |

### Transport behavior

`TCellHost::Start` always bootstraps gRPC first. If the configured transport is
RDMA, it additionally starts an RDMA data endpoint.

| Data transport | Discovery | Mount/unmount/control | Read/write/zero |
|---|---|---|---|
| gRPC | gRPC | gRPC | gRPC |
| RDMA | gRPC | gRPC | RDMA |
| NBD | unsupported by current `TCellHost::Start` | — | — |

For RDMA, configure and start the daemon's RDMA client separately, set a valid
`RdmaPort`, and provision cells RDMA workers. Configuring only
`Transport: CELL_DATA_TRANSPORT_RDMA` is not sufficient.

If a host-level `Transport` is present in `TCellHostConfig`, it overrides the
cell-wide transport. Secure gRPC selection is based on non-zero
`TCellConfig::SecureGrpcPort`; the relevant CA/client certificate paths come
from `GrpcClientConfig`. There is an additional bootstrap condition to account
for: `SetupCellManager` creates a real certificate provider only when
`GrpcClientConfig.SecurePort` is also non-zero. Therefore a TLS setup must
configure both the global client-side secure-port marker/settings and each
peer cell's actual `SecureGrpcPort` consistently.

## Connection and host-selection behavior

At cell-manager start:

1. every configured cell shuffles its unused host list;
2. it begins activating enough hosts to reach `MinCellConnections`;
3. every host creates its gRPC endpoint and, when requested, its RDMA endpoint;
4. `DescribeVolume` uses up to `DescribeVolumeHostCount` active hosts per cell;
5. `GetCellEndpoint` randomly selects one currently active host for a newly
   created endpoint.

An endpoint is then bound to the selected host interfaces. The cell is not
reselected per I/O. Client wrappers such as the durable client can retry
requests, and the underlying client endpoint can reconnect, but this should not
be confused with performing a fresh cross-cell discovery or host selection for
every retry.

Current implementation details worth rechecking before relying on dynamic host
lifecycle behavior:

- `TCellHost::Stop()` and `TCell::Stop()` are currently empty;
- active-host state is established by endpoint bootstrap, while long-term
  health/replacement behavior is limited in this component;
- `TRemoteStorage::AllocateBuffer` returns `nullptr`, `EraseDevice` is not
  implemented, and `ReportIOError` is a no-op.

## Failure and consistency considerations

- A successful discovery reply wins immediately; duplicate disk visibility
  outside the supported relocation/principal mechanism can therefore make
  ownership selection timing-dependent.
- If any configured cell has no active discovery endpoint and no cell succeeds,
  lookup returns retriable `E_REJECTED`, not a definitive `E_NOT_FOUND`.
- `MinCellConnections < DescribeVolumeHostCount` limits the effective discovery
  host count until more hosts become active; the current activation logic targets
  only the minimum connection count.
- A non-empty `SecureGrpcPort` changes all gRPC communication for that host to
  TLS. Certificates and server listener configuration must match.
- RDMA is only the data path. Losing or misconfiguring gRPC still breaks
  discovery and mount/unmount even when RDMA itself is healthy.
- The cell ID in a remote `DescribeVolume` header is checked by the destination
  gRPC server. Mismatched configuration produces `E_REJECTED` and a
  `DescribeVolume response cell id mismatch` error.
- The existence of a cell route does not imply data locality. Inside the owning
  cell, tablet pipes, partitions, and Disk Agents can add further network hops.

## Suggested configuration and rollout checklist

1. Assign a stable, unique `CellId` to every cell.
2. On each NBS endpoint server, set its current `CellId` and list all required
   peer cells with multiple reachable NBS hosts.
3. Verify that `GrpcPort`/`SecureGrpcPort` match the actual NBS listeners on all
   listed hosts.
4. Start with `CELL_DATA_TRANSPORT_GRPC` to validate discovery, mount, unmount,
   and data traffic before introducing RDMA-specific dependencies.
5. For TLS, provision roots and optional client certificates in
   `GrpcClientConfig`, and verify destination server certificates/FQDNs.
6. Size `MinCellConnections` and `DescribeVolumeHostCount`; normally the minimum
   active connection count should not be below the desired discovery fan-out.
7. Enable `CellsEnabled` only after every server has a correct current `CellId`
   and peer mapping.
8. Test from every source cell:
   - local disk discovery and endpoint start;
   - foreign disk discovery and endpoint start;
   - read, write, zero/discard as applicable;
   - stop/unmount;
   - one unavailable peer host;
   - an unavailable entire peer cell;
   - TLS certificate failure;
   - RDMA failure while gRPC remains available, if RDMA is enabled.
9. Observe the `BLOCKSTORE_CELLS` log and the BlockStore `Cells` monitoring page
   for active/activating hosts and discovery errors.

## Useful code map for future sessions

| Question | File/function |
|---|---|
| Where is `--cells-file` parsed? | [`../daemon/common/options.cpp`](../daemon/common/options.cpp), [`../daemon/common/config_initializer.cpp`](../daemon/common/config_initializer.cpp) |
| When is the real cell manager enabled? | [`../daemon/ydb/bootstrap.cpp`](../daemon/ydb/bootstrap.cpp), `SetupCellManager` |
| Where is the current server cell ID installed? | [`../daemon/common/bootstrap.cpp`](../daemon/common/bootstrap.cpp), server creation options |
| Where are peer clients started? | [`impl/cell_manager_impl.cpp`](impl/cell_manager_impl.cpp), `TCellManager::Start` |
| How are active hosts selected? | [`impl/cell_impl.cpp`](impl/cell_impl.cpp), `PickHost`, `PickHosts`, `AdjustActiveHostsToMinConnections` |
| How are gRPC/RDMA endpoints created? | [`impl/endpoint_bootstrap_impl.cpp`](impl/endpoint_bootstrap_impl.cpp) |
| Where does multi-cell discovery happen? | [`impl/describe_volume.cpp`](impl/describe_volume.cpp) |
| Where does `StartEndpoint` consume `cellId`? | [`../endpoints/session_manager.cpp`](../endpoints/session_manager.cpp), `CreateSessionImpl`, `CreateStorageDataClient` |
| Where is the session passed to vhost? | [`../endpoints_vhost/vhost_server.cpp`](../endpoints_vhost/vhost_server.cpp), `TVhostEndpointListener::StartEndpoint` |
| Where does session I/O enter the chosen backend? | [`../endpoints/session_manager.cpp`](../endpoints/session_manager.cpp), `TStorageDataClient` macro-generated methods |
| What is local-cell default storage? | [`../service/storage_provider.cpp`](../service/storage_provider.cpp), `TServiceStorage` |
| What is foreign-cell storage? | [`impl/remote_storage.cpp`](impl/remote_storage.cpp), `TRemoteStorage` |
| Where does NBS choose direct VolumeActor vs tablet pipe? | [`../storage/service/service_actor_forward.cpp`](../storage/service/service_actor_forward.cpp), `TServiceActor::ForwardRequest` |
| Where is the request actually sent through the tablet pipe? | [`../storage/service/volume_client_actor.cpp`](../storage/service/volume_client_actor.cpp), `TVolumeClientActor::HandleRequest` |
| Where is a local Volume Tablet external-booted? | [`../storage/service/volume_session_actor_start.cpp`](../storage/service/volume_session_actor_start.cpp), `TStartVolumeActor` |
| Where does local unmount stop it? | [`../storage/service/volume_session_actor_unmount.cpp`](../storage/service/volume_session_actor_unmount.cpp) |

## Compact handoff context

For a new investigation session, the essential model is:

> NBS cells performs disk discovery and selects an NBS service/storage backend at
> endpoint creation time. `DescribeVolume` fans out to the local service and a
> limited set of active gRPC hosts in every configured peer cell; the first valid
> success supplies `TVolume` and `cellId`. Empty `cellId` creates storage through
> the local `StorageProvider`; non-empty `cellId` selects one configured active
> host and uses gRPC for control plus gRPC or RDMA for data. Both are wrapped by
> the same `TStorageDataClient` and `ISession`, so vhost is unaware of cells. Once
> a request reaches the owning cell's NBS Service, ordinary volume routing takes
> over: direct `VolumeActor` for a locally external-booted Volume Tablet, or
> `VolumeClientActor` plus tablet pipe/YDB interconnect for a Hive-managed tablet.
> Cell locality and Volume Tablet locality are independent.
