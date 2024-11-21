Current CSI Driver implementation violates CSI specification in terms of stage/publish/unstage/unpublish volumes.
At this moment StageVolume step is completely ignored and start endpoint/mounting volumes happens at PublishVolume step.
As a result CSI Driver doesn't support ReadWriteOnce access mode in the correct way and only one pod on the same node can mount the volume,
however it should be allowed to mount the same volume into multiple pods on the same node.

According to CSI Driver specification:

NodeStageVolume should mount the volume to the staging path
NodePublishVolume should mount the volume to the target path
NodeUnpublishVolume should unmount the volume from the target path
NodeUnstageVolume should unmount the volume from the staging path
As we already have current implementation of CSI Driver in production clusters we need to handle migration
from existing implementation of mounting volumes(only NodePublishVolune/NodeUnpublishVolume is implemented)
to the new implementation.

The tricky part here is using different UnixSocketPath/InstanceId/ClientId
for already bounded volumes and "new" volumes.

Current format of UnixSocketPath: socketsDir/podId/volumeId
New format of UnixSocketPath: socketsDir/volumeId

Current format of InstanceId: podId
New format of InstanceId: nodeId

Current format of ClientId: clientId-podId
New format of ClientId: clientId-nodeId

Possible scenarios:

--------
1. Volume was staged and published
2. CSI Driver was updated
3. Volume was unpublished and unstaged <- here we should handle unpublish with old unix socket path
--------
1. Volume was staged and published
2. CSI Driver was updated
3. Kubelet restart happened
4. CSI Driver received stage and publish for the same volume again <- here we should handle stage/publish with old unix socket path
--------
1. CSI Driver was updated
2. Volume was staged and published
3. endpoint should start with new unix socket path
4. Volume was unpublished and unstaged
5. UnstageVolume should stop endpoint with new unix socket path
--------
1. CSI Driver was updated
2. Volume was staged and published on the node #1 with RWO access mode
3. Staging volume on the node #2
4. StageVolume on the node #2 should return error


Migration is splitted for differnt modes
VM mode: https://github.com/ydb-platform/nbs/pull/1982
Mount mode: https://github.com/ydb-platform/nbs/pull/2195
Block mode: https://github.com/ydb-platform/nbs/pull/2269

After migration of all volumes to the new endpoints we can remove backward compatibility
with old format of endpoints.

External links/Documentation:
https://github.com/container-storage-interface/spec/blob/master/spec.md#node-service-rpc