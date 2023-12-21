Cluster Management System (CMS)
--------------------------------------------------------------------------

Terminology:
- Device is a physical disk or a part of a physical disk.
- NRD is a nonreplicated disk or a mirrored disk replica that consists of one or more devices. It has `STORAGE_MEDIA_SSD_NONREPLICATED` [media kind](https://github.com/ydb-platform/nbs/blob/main/cloud/storage/core/protos/media.proto#L16).

There is a regular need to perform maintenance on cluster hosts: replace disks, update the Linux kernel, etc.
But if parts of NRD are located on the host, then one cannot take hosts to maintenance spontaneously - NRD will stop working.

To prevent users from suffering during host maintenance, Disk Registry (DR) is able to migrate NRD to other hosts. Before performing any action with the host or the device, one needs to request permission for this action with the appropriate [request](https://github.com/ydb-platform/nbs/blob/main/cloud/blockstore/public/api/protos/cms.proto).

The response to the request depends on the state the host (or devices) is in, and whether NRDs have devices that are located on the host (or device). DR can either allow the action to be performed (S_OK) or not (E_TRY_AGAIN). If the host or the device is not known to DR, the E_NOT_FOUND error will be returned.

Host [state](https://github.com/ydb-platform/nbs/blob/main/cloud/blockstore/libs/storage/protos/disk.proto#L18):
- `AGENT_STATE_ONLINE` - the host is functioning normally.
- `AGENT_STATE_WARNING` - something has happened to the host or is going to happen (for example, maintenance). No new NRD will be allocated on the host, and those that are will migrate to other hosts.
- `AGENT_STATE_UNAVAILABLE` - the host is unavailable.

Device [state](https://github.com/ydb-platform/nbs/blob/main/cloud/blockstore/libs/storage/protos/disk.proto#L27):
- `DEVICE_STATE_ONLINE` - the device is functioning normally.
- `DEVICE_STATE_WARNING` - something has happened to the device or is going to happen (for example, replacement). No new NRD will be allocated on the device, and those that are will migrate to other hosts.
- `DEVICE_STATE_ERROR` - the device is broken.

Scenarios:
- Host maintenance (REMOVE_HOST)
    If the host is in the `AGENT_STATE_ONLINE` state, then the state changes to `AGENT_STATE_WARNING'.
    If there is no NRD on the host or the host is in the `AGENT_STATE_UNAVAILABLE` state, then the host maintenance is allowed;
    otherwise, the migration process of the dependent NRDs is started. As long as any NRD depends on the host, the host maintenance will be prohibited.

- Physical device replacement (REMOVE_DEVICE)
    It is similar to the case with host maintenance, but the states of the devices change.

- Add hosts (ADD_HOST)
    If the host is in the `AGENT_STATE_WARNING` state, then the state changes to `AGENT_STATE_ONLINE`.
    The operation is allowed unless the host is in the `AGENT_STATE_UNAVAILABLE` state.

- Add disks (ADD_HOST)
    Similar to the case of adding a host to the cluster.

- Adding a new host to the cluster.
    When a new host is added to the cluster, ADD_HOST is called first; after that, ADD_DEVICE is called, indicating the new disks.
    DR remembers the new host and its devices.

Additionally, one can send a GET_DEPENDENT_DISKS request to get a list of NRDs that depend on the specified host.
