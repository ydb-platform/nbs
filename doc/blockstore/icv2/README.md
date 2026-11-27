# IC V2 in BlockStore

IC V2 is a TCP transport for block reads, writes, and zeroing between NBS nodes in different cells. It lets a client use a volume served in another cell. Cells uses gRPC to find and mount the volume and to check remote host health.

Cells creates the outgoing data endpoint through `IDataEndpointFactory`. On the client side, `IPeer` establishes the connection and maintains the `IRequestChannel`. On the server side, `IListener` accepts the TCP connection, and `IDataTarget` forwards incoming operations to the local BlockStore service through `IBlockStore`.

- [Cells client API](cells-api.md)
- [Internal architecture and client/server composition](interfaces.md)
- [Connection establishment, XDC, and fallback](connections.md)
- [Read, write, and zero request flow](requests.md)
