# RDMA architecture
## Read/Write sequence
![Read/Write Sequence](diagrams/rw_sequence.svg)

## [RDMA library](../../cloud/blockstore/libs/rdma)

RDMA library provides interface to communicate
through the RDMA transport. The interface is composed of two entities: `Client`
and `Server`. It allows multiple Clients to connect to a single Server.

### [Client](../../cloud/blockstore/libs/rdma/iface/client.h)

Single client object (created using `NRdma::CreateClient`) can connect to multiple
servers using `StartEndpoint`. After successful connection `Endpoint` can be
used to allocate request (`AllocateRequest`) and send it (`SendRequest`).

Each endpoint pre-allocates `QueueDepth` buffers for request/response
metadata. Additional buffers for request/response data are allocated on demand
using `SendBuffers` or `RecvBuffers` buffer pools kept for each endpoint.

Calling `AllocateRequest` on endpoint will allocate enough buffers for the
request data transfer. After that calling `SendRequest` will send the request
using rdma primitives to the server. After the server responds to the request,
the library will invoke client handler callback which was provided during the
call to the `AllocateRequest`.

### [Server](../../cloud/blockstore/libs/rdma/iface/server.h)

The server object (created using `NRdma::CreateServer`) will start listening for
client connections after invoking `StartEndpoint`. When client connects to the
server new `ServerSession` will be created and the server will start receiving
requests from client. The handler provided in the `StartEndpoint` call will be
invoked on new requests with `in`/`out` buffers which can be used to read/write
data. The handler then invokes `SendResponse` to send response data written to
the `out` buffer. It can signal error by invoking endpoints `SendError` method.

### [RDMA transport](../../cloud/blockstore/libs/rdma/impl)

When `SendRequest` is invoked on the client, the client will send message
consisting of `address`, `length` and `rkey` of the `in`/`out` buffers and the
`request id` to the server. This message will be sent using `IBV_WR_SEND` rdma
verb.

The message will be received on the server side. If the message `in` buffer size
is not zero the server will use `IBV_WR_RDMA_READ` rdma verb to read the
data. If the `out` buffer size is not zero the server will allocate buffer on
the server side for the response. Next the handler will be invoked with the
`in`/`out` buffers which it can use to process request and write response. After
the handler completes, if there is `out` buffer with non zero size, it will be
written using `IBV_WR_RDMA_WRITE` rdma verb. Next the response message will be
sent to the client using `IBV_WR_SEND` rdma verb.

The client will receive the response message and will invoke request handler
signaling completion of request.

## Partition [part_nonrepl_rdma_actor](../../cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_rdma_actor.h)

Non replicated partition rdma actor is used to send read/write requests to disk
agent over rdma. Single `Read/WriteBlocksRequest` will be partition into
multiple requests for specific devices. For each device, the rdma actor will use
rdma lib client to send serialized `Read/WriteDeviceBlocksRequest` protobuf. It
will get response and deserialize it into `Read/WriteDeviceBlocksResponse`.
After all device requests will be completed the single `Read/WriteBlocksRequest`
will be completed.

For write request we will copy buffers once from the `WriteBlocksRequest` to the
`WriteDeviceBlocksRequest` when we serialize the `WriteDeviceBlocksRequest`
before we send it.

For read request we are going to copy the buffer once we receive response from
the `ReadDeviceBlocksResponse` to the `ReadBlocksResponse`.

## Disk agent [rdma_target](../../cloud/blockstore/libs/storage/disk_agent/rdma_target.h)

Disk agent is using rdma\_target to handle read/write requests sent to it using
rdma lib. It initializes rdma server and starts endpoint to listen for
connections. Once connection is established the rdma_target will handle
`Read/WriteDeviceBlocksRequest`.

For `WriteDeviceBlocksRequest`, it constructs `WriteBlocksRequest` using the
same data buffers and writes data using `StorageAdapter`.

For `ReadDeviceBlocksRequest`, it uses `StorageAdapter` to read data into
`ReadBlocksRequest` and then converts response to `ReadDeviceBlocksRequest`.

There is extra buffer copy inside the `StorageAdapter`. We use aio for
read/write and it requires page_size aligned buffers. When we call
`device->Read/WriteBlocks`, the device will allocate aligned buffer, and copy
data to/from it.
