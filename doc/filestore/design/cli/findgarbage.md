# filestore-client findgarbage

Used to find inconsistencies in multishard filestores. It lists contents of the nodeRefs table in the main filesystem and lists all the nodes that are present in the shards. Those entries that are present in only one of the two are considered to be inconsistencies.

Note that this operation is not atomic (traversal of the main filesystem and shards is done sequentially) and thus may not be entirely accurate if the filesystem is being modified while the operation is running. In order to mitigate this problem, stat operations are performed to verify that

This tool has two modes:

* `--find-in-shards`: find nodes that are present in the shards but not in the main filesystem.
* `--find-in-leader`: find nodes that are present in the main filesystem but not in the shards.