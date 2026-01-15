# Repository structure

# Table of Contents
* [Common libs](#common)
* [Common storage libs & tools](#cloud-storage)
* [Blockstore (NBS)](#blockstore-nbs)
* [Filestore](#filestore)
* [Disk Manager](#disk-manager)

## Common
* [util](/util) - the most basic things: collections, strings, smart pointers, basic cross-platform wrappers for OS primitives (like sockets), etc
* [library](/library) - common algorithms and data structures which are not directly related to a specific project
* [contrib](/contrib) - OSS libraries and tools imported into our repo - original LICENSE file and all other original files are kept together with the imported code

## Cloud storage
* [cloud/storage/core](/cloud/storage/core) - common code used by NBS and Filestore
* [config](/cloud/storage/core/config) - config proto specs for common components
* [protos](/cloud/storage/core/protos) - proto specs for common entities like TError, EStorageMediaKind, etc
* [tests](/cloud/storage/core/tests) - scripts for launching the components used in both NBS and Filestore tests - e.g. qemu
* [tools](/cloud/storage/core/tools) - the tools not specific to only NBS or only Filestore

### Cloud storage libs
* [cloud/storage/core/libs](/cloud/storage/core/libs) - the libraries used by both NBS and Filestore
* [actors](/cloud/storage/core/libs/actors) - some helpers for the code that uses ActorSystem
* [aio](/cloud/storage/core/libs/aio) - a more convenient wrapper around libaio (and, possibly, uring in the future)
* [api](/cloud/storage/core/libs/api) - Actor api for the actor-based components which are used both by NBS and Filestore
* [auth](/cloud/storage/core/libs/auth) - Authorized actor
* [common](/cloud/storage/core/libs/common) - Common structures and algorithms like ring buffer, error classification, scatter-gather list (sglist), etc.
* [coroutine](/cloud/storage/core/libs/coroutine) - Convenience wrappers for the coroutine library
* [daemon](/cloud/storage/core/libs/daemon) - Common code which is needed for daemon implementation - base classes for cmdline options, configs, signal handling, mlock, etc
* [diagnostics](/cloud/storage/core/libs/diagnostics) - Monitoring and tracing-related code - metrics aggregation, trace serialization/deserialization and analysis, histograms, some system stats monitoring code
* [features](/cloud/storage/core/libs/features) - Feature flags implementation
* [grpc](/cloud/storage/core/libs/grpc) - Helpers for nontrivial async grpc server/client implementation - executor, channel builders, credentials & auth, keepalive, etc.
* [hive_proxy](/cloud/storage/core/libs/hive_proxy) - Hive Tablet client actor which caches pipes to Hive
* [iam](/cloud/storage/core/libs/iam) - IAM client interface
* [keyring](/cloud/storage/core/libs/keyring) - Contains both the linux keyring-based endpoint storage (together with a convenience wrapper for linux keyring syscall api) and the file-based one 
* [kikimr](/cloud/storage/core/libs/kikimr) - YDB BlobStorage integration code - cluster registration, actorsystem configuration initialization, helpers
* [tablet](/cloud/storage/core/libs/tablet) - Some things common for the implementation of our tablets - TPartialBlobId, garbage collection logic
* [throttling](/cloud/storage/core/libs/throttling) - Request throttling implementation - leaky bucket implementation, throttler policy interface, throttler implementation for actor-based code
* [uds](/cloud/storage/core/libs/uds) - Unix socket listener, unix socket-based client wrapper with a TFuture-based interface
* [user_stats](/cloud/storage/core/libs/user_stats) - Some specific metrics-related code with serialization into json and spack formats
* [version](/cloud/storage/core/libs/version) - The code which can can extract build and repo-related info
* [version_ydb](/cloud/storage/core/libs/version_ydb) - The code which lets us register in the YDB BlobStorage cluster with the correct Application name
* [vhost-client](/cloud/storage/core/libs/vhost-client) - Our [vhost-blk](https://archive.fosdem.org/2023/schedule/event/sds_vhost_user_blk/) client implementation - used mostly in tests
* [viewer](/cloud/storage/core/libs/viewer) - Helpers for outputting some tablet-related info onto tablet monitoring pages

## Blockstore (NBS)
* [blockstore](/cloud/blockstore) - Network Block Store API and implementation - storage node (diskagentd), initiator + control plane + config plane (nbsd), client (blockstore-client), other tools and tests
* [plugin](/cloud/vm) - Legacy Network Block Store qemu plugin which can connect to NBS using either NBD or gRPC APIs
* [blockstore/config](/cloud/blockstore/config) - config proto specs
* [blockstore/public](/cloud/blockstore/public) - gRPC public API specs
* [blockstore/private](/cloud/blockstore/private) - gRPC private API specs - private API actions can be called via the ExecuteAction public API call or via blockstore-client executeaction
* [blockstore/tests](/cloud/blockstore/tests) - integration tests - most of them launch single-node BlobStorage cluster + NBS and then issue some requests and check the responses/metrics/etc
* [vhost-server](/cloud/blockstore/vhost-server) - a helper dataplane-only daemon which can be used by nbsd to offload IO to to get higher performance due to a drastically simpler datapath

### Blockstore apps
* [apps/client](/cloud/blockstore/apps/client) - cmdline client which uses mostly gRPC API - can perform almost any operation which is present in the API of nbsd
* [apps/disk_agent](/cloud/blockstore/apps/disk_agent) - storage node server
* [apps/server](/cloud/blockstore/apps/server) - the main NBS server (nbsd) - provides NBD/vhost-blk/gRPC APIs both for dataplane and controlplane ops, can host volume tablets
* [apps/server_lightweight](/cloud/blockstore/apps/server_lightweight) - nbsd version without YDB BlobStorage dependencies - uses local fs for storage, suitable for local debugging and as an nbsd mock to test your app's integration with nbsd
* [apps/tools](/cloud/blockstore/apps/tools) - misc tools for testing, debugging, log analysis and visualization

### Blockstore libs
* [libs/client](/cloud/blockstore/libs/client) - client interface, gRPC client implementation, durable client (retrying client) implementation, session (remounting client) implementation, throttling client implementation
* [libs/client_rdma](/cloud/blockstore/libs/client_rdma) - client implementation over our RDMA-based transport lib
* [libs/client_spdk](/cloud/blockstore/libs/client_spdk) - client implementation over SPDK
* [libs/common](/cloud/blockstore/libs/common) - NBS-specific common data structures and algorithms
* [libs/daemon](/cloud/blockstore/libs/daemon) - bootstrap code for nbsd and lightweight nbsd
* [libs/diagnostics](/cloud/blockstore/libs/diagnostics) - NBS metrics collection and aggregation and related things
* [libs/discovery](/cloud/blockstore/libs/discovery) - NBS instance discovery and simple balancing
* [libs/disk_agent](/cloud/blockstore/libs/disk_agent) - bootstrap code for diskagentd
* [libs/encryption](/cloud/blockstore/libs/encryption) - data-encrypting wrappers for NBS client and server code
* [libs/endpoints](/cloud/blockstore/libs/endpoints) - endpoint management (StartEndpoint/StopEndpoint/KickEndpoint/etc request processing, concrete IPC listeners implemented in endpoints_$IPC libs)
* [libs/endpoints_grpc](/cloud/blockstore/libs/endpoints_grpc) - gRPC endpoint listener
* [libs/endpoints_nbd](/cloud/blockstore/libs/endpoints_nbd) - NBD endpoint listener
* [libs/endpoints_rdma](/cloud/blockstore/libs/endpoints_rdma) - endpoint listener for our RDMA transport lib
* [libs/endpoints_spdk](/cloud/blockstore/libs/endpoints_spdk) - endpoint listener for SPDK-based endpoints (iSCSI, NVMe-oF)
* [libs/endpoints_vhost](/cloud/blockstore/libs/endpoints_vhost) - vhost-blk endpoint listener
* [libs/kikimr](/cloud/blockstore/libs/kikimr) - YDB BlobStorage integration helpers
* [libs/kms](/cloud/blockstore/libs/kms) - KMS client API and implementation
* [libs/logbroker](/cloud/blockstore/libs/logbroker) - LogBroker (YDB BlobStorage-based persistent queue) client interface and topic API-based implementation
* [libs/nbd](/cloud/blockstore/libs/nbd) - NBD protocol, client and server
* [libs/notify](/cloud/blockstore/libs/notify) - user notification service integration
* [libs/nvme](/cloud/blockstore/libs/nvme) - NVMe specifics - getting device serial numbers, formatting devices, etc
* [libs/rdma](/cloud/blockstore/libs/rdma) - libibverbs and librdmacm-based transport lib, used for transport over RDMA
* [libs/rdma_test](/cloud/blockstore/libs/rdma_test) - mocks for testing the code that uses libs/rdma
* [libs/server](/cloud/blockstore/libs/server) - NBS gRPC server
* [libs/service](/cloud/blockstore/libs/service) - main NBS internal (in-process) APIs like IStorage and IBlockStore + some simple wrappers for them (like FilteredService and ErrorTransformService)
* [libs/service_kikimr](/cloud/blockstore/libs/service) - implementation of the aforementioned APIs over YDB BlobStorage (an ActorSystem adapter actually)
* [libs/service_local](/cloud/blockstore/libs/service_local) - implementation of the aforementioned APIs over local FS - used mostly for debugging and testing purposes
* [libs/service_throttling](/cloud/blockstore/libs/service_throttling) - a throttling wrapper for IBlockStore - can be used for coarse node-level throttling - e.g. to limit the amount of traffic processed by a single gRPC server
* [libs/spdk](/cloud/blockstore/libs/spdk) - interface for everything SPDK-related
* [libs/storage](/cloud/blockstore/libs/storage) - ActorSystem-based block storage layer implementation
* [libs/throttling](/cloud/blockstore/libs/throttling) - NBS-specific throttler wrapper
* [libs/validation](/cloud/blockstore/libs/validation) - checksum-checking wrappers for NBS client and server code - used in test environments
* [libs/vhost](/cloud/blockstore/libs/vhost) - vhost-blk server code
* [libs/ydbstats](/cloud/blockstore/libs/ydbstats) - uploads detailed diagnostics to YDB tables (not BlobStorage but YDB database) for further YQL-based analytics

### Blockstore libs/storage
* [storage/api](/cloud/blockstore/libs/storage/api) - public events (events which are fine to use for inter-component communication) of the actor-based components 
* [storage/bootstrapper](/cloud/blockstore/libs/storage/bootstrapper) - tablet bootstrapper - used by BlockStore Volume tablet to launch BlockStore Partition tablets 
* [storage/core](/cloud/blockstore/libs/storage/core) - things that are used by 2+ storage components, e.g. TStorageConfig, localdb transaction wrappers, TCompactionMap, misc helpers, etc.
* [storage/disk_agent](/cloud/blockstore/libs/storage/disk_agent) - blockstore-disk-agent core part - TDiskAgentActor - implements actor-based API, RDMA API, registers itself in DiskRegistry, etc.
* [storage/disk_common](/cloud/blockstore/libs/storage/disk_common) - common funcs for DiskAgent and DiskRegistry
* [storage/disk_registry](/cloud/blockstore/libs/storage/disk_registry) - DiskRegistry implementation - Agent and Device lists, Actor, Tablet, device allocation logic, migration controller, replication controller, disk placement groups manager, etc. - storage resource registry and allocation for STORAGE_MEDIA_{SSD,HDD}_{NONREPLICATED,MIRROR2,MIRROR3}
* [storage/disk_registry_proxy](/cloud/blockstore/libs/storage/disk_registry_proxy) - a convenience client for DiskRegistry - caches pipes to DiskRegistry tablet
* [storage/init](/cloud/blockstore/libs/storage/init) - ActorSystem bootstrap code for blockstore-server and blockstore-disk-agent
* [storage/model](/cloud/blockstore/libs/storage/model) - things that are used by 2+ storage components - differs from storage/core in that "model" libs shouldn't depend on YDB BlobStorage code (therefore their build times are MUCH lower)
* [storage/partition](/cloud/blockstore/libs/storage/partition) - BlockStore Partition tablet implementation - it is the actual block storage implementation for STORAGE_MEDIA_{SSD,HDD} disks
* [storage/partition2](/cloud/blockstore/libs/storage/partition2) - experimental BlockStore Partition2 tablet implementation (also for STORAGE_MEDIA_{SSD,HDD} disks)
* [storage/partition_common](/cloud/blockstore/libs/storage/partition_common) - common code for the actors that implement block storage - partition, partition2, partition_nonrepl, volume - contains some parts of the implementation of overlay disks, fresh blocks storage, draining, long-running operation tracking, changed blocks tracking 
* [storage/partition_nonrepl](/cloud/blockstore/libs/storage/partition_nonrepl) - block storage implementation of STORAGE_MEDIA_{SSD,HDD}_{NONREPLICATED,MIRROR2,MIRROR3}
* [storage/perf](/cloud/blockstore/libs/storage/perf) - a really tiny amount of benchmarks (which should probably be moved to storage/partition2)
* [storage/protos](/cloud/blockstore/libs/storage/protos) - internal proto specs which don't depend on YDB BlobStorage proto specs
* [storage/protos_ydb](/cloud/blockstore/libs/storage/protos_ydb) - internal proto specs which do depend on YDB BlobStorage proto specs
* [storage/service](/cloud/blockstore/libs/storage/service) - TServiceActor - our entry point to the actor-based component environment - handles disk creation/destruction/resizing/altering requests, forwards IO events to TVolumeActors (can pass events to local volumes and to remote volumes as well), aggregates service-layer metrics
* [storage/ss_proxy](/cloud/blockstore/libs/storage/ss_proxy) - a convenience client for schemeshard - used in the disk creation/destruction/resizing/altering logic
* [storage/stats_service](/cloud/blockstore/libs/storage/stats_service) - stats aggregation actor
* [storage/testlib](/cloud/blockstore/libs/storage/testlib) - helpers and mocks for actor uts
* [storage/undelivered](/cloud/blockstore/libs/storage/undelivered) - an actor which can cancel all public NBS events - may be used to handle undelivered requests in a generic way
* [storage/volume](/cloud/blockstore/libs/storage/volume) - BlockStore Volume tablet implementation - entry point for all requests for a single NBS disk
* [storage/volume_balancer](/cloud/blockstore/libs/storage/volume_balancer) - a component which tracks some metrics like CpuWait, detects blockstore-server overload and releases Hive locks for some of the locally mounted Volume tablets (so that they get restarted by Hive on other nodes thus reducing the load on the current blockstore-server node) 
* [storage/volume_proxy](/cloud/blockstore/libs/storage/volume_proxy) - a convenience client for all BlockStore Volumes which caches pipes to Volume tablets                    

### Blockstore tools
* [tools/analytics](/cloud/blockstore/tools/analytics) - various visualization, dumping and statistics calculation
* [tools/ci](/cloud/blockstore/tools/ci) - tools for e2e testing on real clusters
* [tools/cms](/cloud/blockstore/tools/cms) - tools for configuring NBS via YDB CMS
* [tools/csi_driver](/cloud/blockstore/tools/csi_driver) - [K8s CSI](https://kubernetes-csi.github.io/docs/) implementation for NBS & Filestore
* [tools/debug](/cloud/blockstore/tools/debug) - some debugging stuff like e.g. pretty-printers
* [tools/fs](/cloud/blockstore/tools/fs) - filesystem analytics for common filesystems (e.g. ext4, xfs)
* [tools/http_proxy](/cloud/blockstore/tools/http_proxy) - HTTP/1.1 API over NBS gRPC API
* [tools/nbd](/cloud/blockstore/tools/nbd) - a tool which lets you connect NBS volume as an NBD device
* [tools/testing](/cloud/blockstore/tools/testing) - testing mocks & load generators / response validators used in integration tests

## Filestore

* [filestore](/cloud/filestore) - Network File Store API and implementation - virtio-fs server (filestore-vhost), fs backend server (filestore-server), cmdline client (filestore-client)
* [filestore/config](/cloud/filestore/config) - config proto specs
* [filestore/public](/cloud/filestore/public) - gRPC public API specs
* [filestore/private](/cloud/filestore/private) - gRPC private API specs - private API actions can be called via the ExecuteAction public API call or via filestore-client executeaction
* [filestore/tests](/cloud/filestore/tests) - integration tests - most of them launch single-node BlobStorage cluster + NBS and then issue some requests and check the responses/metrics/etc

### Filestore apps
* [apps/client](/cloud/filestore/apps/client) - cmdline client which uses mostly gRPC API - can perform almost any operation which is present in the API of filestore-vhost and filestore-server, can mount Filestore-based filesystems via FUSE
* [apps/server](/cloud/filestore/apps/server) - filestore-server - provides full gRPC API, contains the actual filesystem implementation and hosts Filestore tablets
* [apps/vhost](/cloud/filestore/apps/vhost) - filestore-vhost - can serve filesystems via virtio-fs or FUSE, sends requests to filestore-server, can read/write data directly from/to YDB BlobStorage groups (to transfer the data directly between BlobStorage and the VM host without passing it through Filestore tablets)

### Filestore libs
* [libs/client](/cloud/filestore/libs/client) - similar to blockstore/libs/client - gRPC client implementation, retrying client implementation, session implementation (but Filestore sessions are stateful unlike Blockstore sessions)
* [libs/daemon](/cloud/filestore/libs/daemon) - bootstrap code for filestore-server and filestore-vhost
* [libs/diagnostics](/cloud/filestore/libs/diagnostics) - Filestore metrics collection and aggregation and related things
* [libs/endpoint](/cloud/filestore/libs/endpoint) - similar to blockstore/libs/endpoint but for Filestore we have only endpoint_vhost listener
* [libs/server](/cloud/filestore/libs/server) - Filestore gRPC server implementation
* [libs/service](/cloud/filestore/libs/service) - main Filestore internal (in-process) APIs like IFileStore + some helpers (e.g. error builders like ErrorIsNotDirectory, ErrorInvalidTarget, etc, other helpers like LockTypeToFcntlMode)
* [libs/service_kikimr](/cloud/filestore/libs/service) - implementation of the aforementioned APIs over YDB BlobStorage (an ActorSystem adapter actually)
* [libs/service_local](/cloud/filestore/libs/service_local) - implementation of the aforementioned APIs over local FS - used mostly for debugging and testing purposes
* [libs/service_null](/cloud/filestore/libs/service_null) - implementation of the aforementioned APIs which returns empty responses - used mostly for debugging and testing purposes
* [libs/storage](/cloud/filestore/libs/storage) - ActorSystem-based file storage layer implementation
* [libs/vfs](/cloud/filestore/libs/vfs) - IFileSystemLoop interface, some helper classes and funcs for FUSE interface implementation
* [libs/vfs_fuse](/cloud/filestore/libs/vfs_fuse) - IFileSystemLoop implementation for FUSE and virtio-fs (based on virtiofsd code), used in filestore-vhost
* [libs/vhost](/cloud/filestore/libs/vhost) - vhost server and client implementation, used in filestore-vhost

### Filestore libs/storage
* [storage/api](/cloud/filestore/libs/storage/api) - public events (events which are fine to use for inter-component communication) of the actor-based components 
* [storage/core](/cloud/filestore/libs/storage/core) - things that are used by 2+ storage components, e.g. TStorageConfig, localdb transaction wrappers, misc helpers, etc.
* [storage/init](/cloud/filestore/libs/storage/init) - ActorSystem bootstrap code for filestore-server and filestore-vhost
* [storage/model](/cloud/filestore/libs/storage/model) - things that are used by 2+ storage components - differs from storage/core in that "model" libs shouldn't depend on YDB BlobStorage code (therefore their build times are MUCH lower)
* [storage/perf](/cloud/filestore/libs/storage/perf) - some benchmarks (currently there is only TCompactionMap benchmark there)
* [storage/service](/cloud/filestore/libs/storage/service) - TStorageServiceActor - our entry point to the actor-based component environment - handles filesystem creation/destruction/resizing/altering requests, sends IO events to local/remote TIndexTabletActors via TIndexTabletProxy, aggregates service-layer metrics
* [storage/ss_proxy](/cloud/filestore/libs/storage/ss_proxy) - a convenience client for schemeshard - used in the filesystem creation/destruction/resizing/altering logic
* [storage/tablet](/cloud/filestore/libs/storage/tablet) - IndexTablet implementation - actual filesystem implementation which handles all inode-layer requests and block/byte-layer requests
* [storage/tablet_proxy](/cloud/filestore/libs/storage/tablet_proxy) - a convenience client for all FileStore tablets which caches pipes to FileStore tablets
* [storage/testlib](/cloud/filestore/libs/storage/testlib) - helpers and mocks for actor uts

### Filestore tools
[cloud/filestore/tools](/cloud/filestore/tools) - the layout is similar to [Blockstore tools](#blockstore-tools)

## Disk Manager

Snapshot & Image service, control plane over NBS & Filestore. Has gRPC-based public API, stores its state in YDB.

[cloud/disk_manager](/cloud/disk_manager) - Disk Manager code
[cloud/disk_manager/api](/cloud/disk_manager/api) - Disk Manager gRPC API
[cloud/disk_manager/test](/cloud/disk_manager/test) - Integration test environment setup recipes/mocks & e2e test runners
[cloud/tasks](/cloud/tasks) - Go task processor (over YDB) - the foundation of Disk Manager

## Disk Manager cmd
* [cmd/disk-manager](cloud/disk_manager/cmd/disk-manager) - Disk Manager server
* [cmd/disk-manager-admin](cloud/disk_manager/cmd/disk-manager-admin) - Disk Manager cmdline client
* [cmd/disk-manager-init-db](cloud/disk_manager/cmd/disk-manager-init-db) - Disk Manager YDB schema initializer

## Disk Manager pkg

TODO

## Disk Manager internal/pkg

* [accounting](/cloud/disk_manager/internal/pkg/accounting) - operation metrics
* [auth](/cloud/disk_manager/internal/pkg/auth) - auth lib
* [client](/cloud/disk_manager/internal/pkg/client) - private API client, used mostly in tests
* [common](/cloud/disk_manager/internal/pkg/common) - common data structures, e.g. ChannelWithCancellation
* [configs](/cloud/disk_manager/internal/pkg/configs) - config proto specs
* [dataplane](/cloud/disk_manager/internal/pkg/dataplane) - dataplane tasks implementation - image import, snapshotting, disk initialization from snapshot, etc.
* [facade](/cloud/disk_manager/internal/pkg/facade) - gRPC API entry point
* [headers](/cloud/disk_manager/internal/pkg/headers) - gRPC Context Metadata helpers
* [health](/cloud/disk_manager/internal/pkg/health) - various Disk Manager instance healthchecks which check boot disk health, Disk Manager <-> NBS connectivity, etc
* [monitoring](/cloud/disk_manager/internal/pkg/monitoring) - metrics HTTP/1.1 server
* [resources](/cloud/disk_manager/internal/pkg/resources) - resource (Image/Snapshot/Disk/Filesystem/etc) metadata storage implementation over YDB
* [services](/cloud/disk_manager/internal/pkg/services) - gRPC services implementation
* [types](/cloud/disk_manager/internal/pkg/types) - proto specs of the internally used data types
* [util](/cloud/disk_manager/internal/pkg/util) - helper code (less generic than internal/pkg/common) - e.g. task state pretty-printers
