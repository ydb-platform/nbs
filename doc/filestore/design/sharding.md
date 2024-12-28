# Filestore Sharding

## CRUD and Session Management
![crud_and_sessions](../excalidraw/sharded_filesystem_crud_and_sessions.svg)

## Node creation, unlink, rename
![node_management](../excalidraw/sharded_filesystem_node_management.svg)

## Reading and writing

TODO

## Metrics aggregation

TODO

## Main tasks
* Initial sharding implementation: https://github.com/ydb-platform/nbs/issues/1350
* Load balancing by the main tablet: https://github.com/ydb-platform/nbs/issues/2559
* Ability to create directory inodes in shards: https://github.com/ydb-platform/nbs/issues/2674
* Scaling reads and writes within a single file: https://github.com/ydb-platform/nbs/issues/2454

## Known issues
* https://github.com/ydb-platform/nbs/issues/2667
