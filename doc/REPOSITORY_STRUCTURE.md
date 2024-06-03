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

TODO

## Filestore

TODO

## Disk Manager

TODO
