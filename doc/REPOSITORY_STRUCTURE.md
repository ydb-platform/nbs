# Repository structure

## Common
[util](/util) - the most basic things: collections, strings, smart pointers, basic cross-platform wrappers for OS primitives (like sockets), etc

[library](/library) - common algorithms and data structures which are not directly related to a specific project

[contrib](/contrib) - OSS libraries and tools imported into our repo - original LICENSE file and all other original files are kept together with the imported code

## Cloud storage
[cloud/storage/core](/cloud/storage/core) - common code used by NBS and Filestore

[config](/cloud/storage/core/config) - config proto specs for common components

[protos](/cloud/storage/core/protos) - proto specs for common entities like TError, EStorageMediaKind, etc

[tests](/cloud/storage/core/tests) - scripts for launching the components used in both NBS and Filestore tests - e.g. qemu

[tools](/cloud/storage/core/tools) - the tools not specific to only NBS or only Filestore

### Cloud storage libs
[cloud/storage/core/libs](/cloud/storage/core/libs) - the libraries used by both NBS and Filestore

[actors](/cloud/storage/core/libs/actors) - some helpers for the code that uses ActorSystem

[aio](/cloud/storage/core/libs/aio) - a more convenient wrapper around libaio (and, possibly, uring in the future)

[api](/cloud/storage/core/libs/api) - Actor api for the actor-based components which are used both by NBS and Filestore

[auth](/cloud/storage/core/libs/auth) - Authorized actor

[common](/cloud/storage/core/libs/common) - Common structures and algorithms like ring buffer, error classification, scatter-gather list (sglist), etc.

[coroutine](/cloud/storage/core/libs/coroutine) - Convenience wrappers for the coroutine library

[daemon](/cloud/storage/core/libs/daemon) - Common code which is needed for daemon implementation - base classes for cmdline options, configs, signal handling, mlock, etc

[diagnostics](/cloud/storage/core/libs/diagnostics) - Monitoring and tracing-related code - metrics aggregation, trace serialization/deserialization and analysis, histograms, some system stats monitoring code

[features](/cloud/storage/core/libs/features) - Feature flags implementation

[grpc](/cloud/storage/core/libs/grpc) - Helpers for nontrivial async grpc server/client implementation - executor, channel builders, credentials & auth, keepalive, etc.

[hive_proxy](/cloud/storage/core/libs/hive_proxy) - Hive Tablet client actor which caches pipes to Hive

[iam](/cloud/storage/core/libs/iam) - IAM client interface

[keyring](/cloud/storage/core/libs/keyring) - Contains both the linux keyring-based endpoint storage (together with a convenience wrapper for linux keyring syscall api) and the file-based one 

[kikimr](/cloud/storage/core/libs/kikimr) - YDB BlobStorage integration code - cluster registration, actorsystem configuration initialization, helpers

[tablet](/cloud/storage/core/libs/tablet) - Some things common for the implementation of our tablets - TPartialBlobId, garbage collection logic

[throttling](/cloud/storage/core/libs/throttling) - Request throttling implementation - leaky bucket implementation, throttler policy interface, throttler implementation for actor-based code

[uds](/cloud/storage/core/libs/uds) - Unix socket listener, unix socket-based client wrapper with a TFuture-based interface

[user_stats](/cloud/storage/core/libs/user_stats) - Some specific metrics-related code with serialization into json and spack formats

[version](/cloud/storage/core/libs/version) - The code which can can extract build and repo-related info

[version_ydb](/cloud/storage/core/libs/version_ydb) - The code which lets us register in the YDB BlobStorage cluster with the correct Application name

[vhost-client](/cloud/storage/core/libs/vhost-client) - Our [vhost-blk](https://archive.fosdem.org/2023/schedule/event/sds_vhost_user_blk/) client implementation - used mostly in tests

[viewer](/cloud/storage/core/libs/viewer) - Helpers for outputting some tablet-related info onto tablet monitoring pages

## Blockstore (NBS)
[blockstore](/cloud/blockstore) - Network Block Store API and implementation - storage node (diskagentd), initiator + control plane + config plane (nbsd), client (blockstore-client), other tools and tests

[plugin](/cloud/vm) - Legacy Network Block Store qemu plugin which can connect to NBS using either NBD or gRPC APIs

[blockstore/config](/cloud/blockstore/config) - config proto specs

[blockstore/public](/cloud/blockstore/public) - gRPC public API specs

[blockstore/private](/cloud/blockstore/private) - gRPC private API specs - private API actions can be called via the ExecuteAction public API call or via blockstore-client executeaction

[blockstore/tests](/cloud/blockstore/tests) - integration tests - most of them launch single-node BlobStorage cluster + NBS and then issue some requests and check the responses/metrics/etc

[vhost-server](/cloud/blockstore/vhost-server) - a helper dataplane-only daemon which can be used by nbsd to offload IO to to get higher performance due to a drastically simpler datapath

### Blockstore apps
[apps/client](/cloud/blockstore/apps/client) - cmdline client which uses mostly gRPC API - can perform almost any operation which is present in the API of nbsd

[apps/disk_agent](/cloud/blockstore/apps/disk_agent) - storage node server

[apps/endpoint_proxy](/cloud/blockstore/apps/endpoint_proxy) - a proxy which can maintain an NBD connection with the kernel and proxy the requests to nbsd (to allow nbsd updates without NBD device reconfiguration)

[apps/server](/cloud/blockstore/apps/server) - the main NBS server (nbsd) - provides NBD/vhost-blk/gRPC APIs both for dataplane and controlplane ops, can host volume tablets

[apps/server_lightweight](/cloud/blockstore/apps/server_lightweight) - nbsd version without YDB BlobStorage dependencies - uses local fs for storage, suitable for local debugging and as an nbsd mock to test your app's integration with nbsd

[apps/tools](/cloud/blockstore/apps/tools) - misc tools for testing, debugging, log analysis and visualization

### Blockstore libs
[libs/client](/cloud/blockstore/libs/client) - client interface, gRPC client implementation, durable client (retrying client) implementation, session (remounting client) implementation, throttling client implementation

[libs/client_rdma](/cloud/blockstore/libs/client_rdma) - client implementation over our RDMA-based transport lib

[libs/client_spdk](/cloud/blockstore/libs/client_spdk) - client implementation over SPDK

[libs/common](/cloud/blockstore/libs/common) - NBS-specific common data structures and algorithms

[libs/daemon](/cloud/blockstore/libs/daemon) - bootstrap code for nbsd and lightweight nbsd

[libs/diagnostics](/cloud/blockstore/libs/diagnostics) - NBS metrics collection and aggregation and related things

[libs/discovery](/cloud/blockstore/libs/discovery) - NBS instance discovery and simple balancing

[libs/disk_agent](/cloud/blockstore/libs/disk_agent) - bootstrap code for diskagentd

[libs/encryption](/cloud/blockstore/libs/encryption) - data-encrypting wrappers for NBS client and server code

[libs/endpoint_proxy](/cloud/blockstore/libs/endpoint_proxy) - blockstore-endpoint-proxy implementation and a client lib for it

[libs/endpoints](/cloud/blockstore/libs/endpoints) - endpoint management (StartEndpoint/StopEndpoint/KickEndpoint/etc request processing, concrete IPC listeners implemented in endpoints_$IPC libs)

[libs/endpoints_grpc](/cloud/blockstore/libs/endpoints_grpc) - gRPC endpoint listener

[libs/endpoints_nbd](/cloud/blockstore/libs/endpoints_nbd) - NBD endpoint listener

[libs/endpoints_rdma](/cloud/blockstore/libs/endpoints_rdma) - endpoint listener for our RDMA transport lib

[libs/endpoints_spdk](/cloud/blockstore/libs/endpoints_spdk) - endpoint listener for SPDK-based endpoints (iSCSI, NVMe-oF)

[libs/endpoints_vhost](/cloud/blockstore/libs/endpoints_vhost) - vhost-blk endpoint listener

[libs/kikimr](/cloud/blockstore/libs/kikimr) - YDB BlobStorage integration helpers

[libs/kms](/cloud/blockstore/libs/kms) - KMS client API and implementation

[libs/logbroker](/cloud/blockstore/libs/logbroker) - LogBroker (YDB BlobStorage-based persistent queue) client interface and topic API-based implementation

[libs/nbd](/cloud/blockstore/libs/nbd) - NBD protocol, client and server

[libs/notify](/cloud/blockstore/libs/notify) - user notification service integration

[libs/nvme](/cloud/blockstore/libs/nvme) - NVMe specifics - getting device serial numbers, formatting devices, etc

[libs/rdma](/cloud/blockstore/libs/rdma) - libibverbs and librdmacm-based transport lib, used for transport over RDMA

[libs/rdma_test](/cloud/blockstore/libs/rdma_test) - mocks for testing the code that uses libs/rdma

[libs/server](/cloud/blockstore/libs/server) - NBS gRPC server

[libs/service](/cloud/blockstore/libs/service) - main NBS internal (in-process) APIs like IStorage and IBlockStore + some simple wrappers for them (like FilteredService and ErrorTransformService)

[libs/service_kikimr](/cloud/blockstore/libs/service) - implementation of the aforementioned APIs over YDB BlobStorage (an ActorSystem adapter actually)

[libs/service_local](/cloud/blockstore/libs/service_local) - implementation of the aforementioned APIs over local FS - used mostly for debugging and testing purposes

[libs/service_throttling](/cloud/blockstore/libs/service_throttling) - a throttling wrapper for IBlockStore - can be used for coarse node-level throttling - e.g. to limit the amount of traffic processed by a single gRPC server

[libs/spdk](/cloud/blockstore/libs/spdk) - interface for everything SPDK-related

[libs/storage](/cloud/blockstore/libs/storage) - ActorSystem-based block storage layer implementation

[libs/throttling](/cloud/blockstore/libs/throttling) - NBS-specific throttler wrapper

[libs/validation](/cloud/blockstore/libs/validation) - checksum-checking wrappers for NBS client and server code - used in test environments

[libs/vhost](/cloud/blockstore/libs/vhost) - vhost-blk server code

[libs/ydbstats](/cloud/blockstore/libs/ydbstats) - uploads detailed diagnostics to YDB tables (not BlobStorage but YDB database) for further YQL-based analytics

### Blockstore libs/storage

TODO

### Blockstore tools
[tools/analytics](/cloud/blockstore/tools/analytics) - various visualization, dumping and statistics calculation

[tools/ci](/cloud/blockstore/tools/ci) - tools for e2e testing on real clusters

[tools/cms](/cloud/blockstore/tools/cms) - tools for configuring NBS via YDB CMS

[tools/csi_driver](/cloud/blockstore/tools/csi_driver) - [K8s CSI](https://kubernetes-csi.github.io/docs/) implementation for NBS & Filestore

[tools/debug](/cloud/blockstore/tools/debug) - some debugging stuff like e.g. pretty-printers

[tools/fs](/cloud/blockstore/tools/fs) - filesystem analytics for common filesystems (e.g. ext4, xfs)

[tools/http_proxy](/cloud/blockstore/tools/http_proxy) - HTTP/1.1 API over NBS gRPC API

[tools/nbd](/cloud/blockstore/tools/nbd) - a tool which lets you connect NBS volume as an NBD device

## Filestore

TODO

## Disk Manager

TODO
