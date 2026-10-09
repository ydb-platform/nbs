# Cross-shard linked disk copy

This extends the existing NBS linked-volume copy workflow to storage shards in one availability zone and one YDB cluster. It reuses background copying, mirrored writes, automatic leadership transfer and exact deletion of the old physical volume. The initial supported cross-shard configuration is replicated network SSD/HDD, with unchanged creation parameters except an optional media kind change.

## Configuration

Every participating NBS node must use the same allowlisted shard identifiers and directory mapping. For example, on the source node:

~~~protobuf
SchemeShardDir: "/Root/nbs-source"
ShardDirectories {
    key: "source"
    value: "/Root/nbs-source"
}
ShardDirectories {
    key: "target"
    value: "/Root/nbs-target"
}
~~~

On the destination node, use the same mapping and set `SchemeShardDir` to `"/Root/nbs-target"`. Directories must be absolute canonical YDB paths. Trailing slashes are normalized. Unknown identifiers and paths containing relative components are rejected.

An empty shard identifier selects the receiving node's local shard. A cross-shard link therefore requires both `LeaderShardId` and `FollowerShardId`. `Headers.ShardId` addresses the destination shard of a particular request; `Headers.CellId` is not a destination selector.

## Prepare the destination

Resolve the active source's physical identifier and use the existing `CreateVolume` workflow to create its alternate physical name in the target shard. Preserve the source creation configuration; the postfix alone does not copy resource metadata. Select a different media kind only when requested.

For an active `disk`, the destination name is `disk-copy`. For an active `disk-copy`, the next name is `disk`. Do not invent a new postfix when retrying an existing operation.

The addressing portion of a destination creation request is:

~~~protobuf
Headers {
    ShardId: "target"
}
DiskId: "disk-copy"
~~~

Include the source's remaining creation parameters in that request. Cross-shard creation through a source node rejects DiskRegistry-based media kinds before allocation; existing local creation remains supported.

## Start and inspect the copy

The existing command accepts optional shard arguments:

~~~bash
blockstore-client createvolumelink \
    --leader-disk-id disk \
    --follower-disk-id disk-copy \
    --leader-shard-id source \
    --follower-shard-id target
~~~

The command is sent to an NBS endpoint that has the configured mapping. Both physical names are resolved exactly in their respective shards.

Inspect the same link, including after a component restart:

~~~bash
blockstore-client executeaction \
    --action GetLinkStatus \
    --input-bytes '{"LeaderDiskId":"disk","LeaderShardId":"source","FollowerDiskId":"disk-copy","FollowerShardId":"target"}'
~~~

A repeated create request for the same active link is idempotent. Query the existing link before starting another operation after an ambiguous response.

`LINK_STATUS_PREPARING` means copying is in progress. `LINK_STATUS_LEADERSHIP_TRANSFERRED` means the destination is authoritative, but old-source cleanup is still pending. `LINK_STATUS_COMPLETED` confirms that the old source has been deleted and the destination recorded that result. A tablet restart can require an idempotent cleanup retry.

Disconnected disks copy without a user mount: source partitions are retained in a copy-only mode while data is needed. Cancellation releases that retention.

## Cancellation

Cancel before leadership transfer using the existing command:

~~~bash
blockstore-client destroyvolumelink \
    --leader-disk-id disk \
    --follower-disk-id disk-copy \
    --leader-shard-id source \
    --follower-shard-id target
~~~

The service requests an atomic cancellation-boundary check in the owning volume tablet. Once transfer starts, cancellation returns `E_INVALID_STATE` and leaves the authoritative relationship intact. The same protection applies when the source has already disappeared and the request is checked on the destination.

Removing a link does not delete the partially filled destination volume. The internal caller must dispose of that destination separately when cancelling. Address its shard and use an exact physical name for that cleanup:

~~~protobuf
Headers {
    ShardId: "target"
    ExactDiskIdMatch: true
}
DiskId: "disk-copy"
~~~

This is the addressing portion of a `DestroyVolume` request.

## Routing after transfer

Subsequent requests must select the destination shard, either through `Headers.ShardId` or an endpoint for that shard. The logical name remains accepted by the destination shard's existing alternate-name lookup. For example:

~~~protobuf
Headers {
    ShardId: "target"
}
DiskId: "disk"
~~~

This is a complete `DescribeVolume` request. Exact-name matching must remain disabled when resolving the logical name.

This operation does not add cluster-wide disk discovery or update Disk Manager or Compute placement metadata. The internal caller remains responsible for routing and any integration-level resource metadata changes. Normal SDK session recovery/remount is still required after a tablet restart.

## Verification

The test coverage includes:

- existing requests without shard fields;
- known, unknown and invalid shard mappings, including local aliases;
- same physical names in different shards and independent proxy caches;
- schema operations with and without SchemeCache, including backup reads;
- SSD-to-SSD, HDD-to-HDD, SSD-to-HDD and HDD-to-SSD copies;
- detached source volumes and foreground writes/zeroes during copy;
- repeated link creation, cancellation and late-cancellation rejection;
- source, destination and simultaneous tablet restarts with copy I/O pending;
- actual old-source deletion and the committed final link state;
- repeat copying back to the original shard with alternate names;
- preservation of data and destination configuration.

Build the server and administrative client:

~~~bash
./ya make -j16 cloud/blockstore/apps/server cloud/blockstore/apps/client
~~~

Run the related regression suites:

~~~bash
./ya make -tA -j16 \
    cloud/blockstore/libs/storage/core/ut \
    cloud/blockstore/libs/storage/ss_proxy/ut \
    cloud/blockstore/libs/storage/volume_proxy/ut \
    cloud/blockstore/libs/storage/service/ut \
    cloud/blockstore/libs/storage/volume/ut \
    cloud/blockstore/libs/storage/volume/ut_linked \
    cloud/blockstore/libs/storage/volume/actors/ut \
    cloud/blockstore/libs/storage/volume/model/ut \
    cloud/blockstore/apps/client/lib/ut
~~~
