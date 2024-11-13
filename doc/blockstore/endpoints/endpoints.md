# Purpose

Endpoint is an abstraction for interaction with existing volumes.
StartEndpoint creates unix socket which is used for communitcation
between nbs server and client to confgiure specified device(e.g. nbd/vhost)

# GRPC API

Available methods:

---

1. ListEndpoints

* The ListEndpoints response includes the EndpointsWereRestored flag,
which indicates whether the persistent endpoints have already been restored
after restart of the blockstore server.

2. StartEndpoint

* UnixSocketPath is mandatory parameter to start new endpoint.
* Persistent flag must be specified to restore endpoint after blockstore-server restart.

3. StopEndpoint

* UnixSocketPath is used as an "endpoint id" to find the endpoint.

4. DescribeEndpoint

* UnixSocketPath is used as an "endpoint id" to find the endpoint.
* Returns the client performance profile.

5. RefreshEndpoint

* UnixSocketPath is used as an "endpoint id" to find the endpoint.
* Reconfigure the underlying device with the new volume size.
* This method is required to enable online volume resizing without the need to recreate endpoints.

6. KickEndpoint

* keyring ID is used to locate the endpoint infromation in the endpoint storage.
* Reload information about endpoint from the endpoint storage.

---

Detailed information about all parameters is available in [proto file](https://github.com/ydb-platform/nbs/blob/main/cloud/blockstore/public/api/protos/endpoints.proto)

# Restoring endpoints

NBS server restores all active persistent endpoints in case of server restart.
Information about active endpoints are stored in the file or keyring.


# Mount conflicts
It is not possible to start two different Read-Write endpoints for the same volume.
In this case StartEndpoint returns MOUNT_CONFLICT error.
