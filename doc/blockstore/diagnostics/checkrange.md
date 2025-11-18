# Utility checkarange

## Description

This is a part of the blockstore-client application and is launched in the standard way for it: `blockstore-client checkrange`. The utility calculates the checksums of each requested block and checks the integrity of the data of the specified blocks for each supported disk type:
- non replicated disk (data reading):
  - calculate checksums;
  - check the possibility of reading.
- mirror2 / mirror3 (data reading + replica checksums verification):
  - check data reading from each replica;
  - compare and display checksums of data between replicas.
-  ydb-based disks:
  - check data reading (YDB will have its own implicit integrity check);
  - TODO: a separate command for SSD disk verification is planned in YDB — https://github.com/ydb-platform/ydb/issues/8189.

## Features
The utility supports the following options:

### Always performed
- a request to calculate the checksum of each specified block. For example, for a range of 1024 blocks, 1024 checksums will be returned.

### Mandatory parameters
- "disk-id" — disk identifier specification.

### Standard launch options
- "start-index" — starting block index. Default: 0.
- "blocks-count" — number of blocks. Default: the number of blocks to the end of the disk, starting from start-index.
- "blocks-per-request" — the number of blocks in 1 request. Default: 1024.
- "output" — file to save the results. Default: stdout.

## Basic use cases
- data migration and checksum comparison:
  - stop writing to the disk;
  - calculate checksums;
  - migrate data to another disk (maybe even of a different type);
  - compare checksums.

## Example parser
 - example Python parser can be found in load tests: nbs/cloud/blockstore/tests/loadtest/local-checkrange/test.py

## Response format
```json
{
  "Ranges": [
    {
      "Start": 0,
      "End": 1023,
      "Error": {}, // if there was an error
      "Checksums": [], // if they were obtained
      "MirrorChecksums": [ // for mirror disks in case of checksum difference for at least 2 replicas
        {
          "ReplicaName": "name",
          "Checksums": [1, 2, 3, 4, 5]
        }
      ]
    }
  ],
  "Summary": {
    "RequestsNum": 1,
    "ErrorsNum": 0,
    "ProblemRanges": [ // if there were errors in 2 consecutive ranges, they are merged into 1 large one
      {
        "Start": 0,
        "End": 2046
      }
    ],
    "GlobalError": {
      "Code": int,
      "CodeString": string,
      "Message": string,
      "Flags": int
    }
  }
}
```

## Internal structure / logic of work
### Utility's side
* getting disk information:
  * based on the disk size and the initially requested number of blocks, the required number of blocks is set;
* for each range of blocks, a checkRange request is sequentially executed. It will then be processed by the corresponding actor on the server (for example, the starting point of processing may be part_mirror_actor_checkrange.cpp: HandleCheckRange);
* response processing:
  * checking «high-level» argument processing errors;
  * checking read errors;
  * setting checksums for each required range.

### Server's side

#### Nonreplicated disk
* TNonreplicatedPartitionActor::HandleCheckRange:
  * checks that the requested number of blocks is less than the maximum allowed size;
  * registers the actor TNonreplCheckRangeActor, which works through the methods of the parent actor (in partition_common).

#### Mirror disks
* TMirrorPartitionActor::HandleCheckRange:
  * checks that the requested number of blocks is less than the maximum allowed size;
  * registers the actor TMirrorCheckRangeActor, passes the replica names (DeviceUUID) to it;
  * TMirrorCheckRangeActor:
    * SendReadBlocksRequest: sends read requests for the specified blocks from each replica of the mirror (TEvReadBlocksRequest).
      * TMirrorPartitionActor::HandleReadBlocks (part_mirror_actor_readblocks.cpp): request processing for reading -> TMirrorPartitionActor::ReadBlocks:
      * the desired replica is selected from the headers;
      * the TRequestActor<TReadBlocksMethod> is created to read from the desired replica;
        * SendRequests is called from the actor created on the previous step:
          * response processing occurs in HandleResponse, eventually we set the checksum and return the response;
    * the response processing (TMirrorCheckRangeActor::HandleReadBlocksResponse):
      * check read errors from the replica;
      * calculate the checksums of the range;
      * prepare the response with 1 general checksum or a set of checksums if there were discrepancies.

#### ydb-based disks
* TPartitionActor::HandleCheckRange:
  * checks that the requested number of blocks is less than the maximum allowed size;
  * registers the actor TCheckRangeActor (base class for other CheckRangeActors).

  * TCheckRangeActor: Bootstrap + SendReadBlocksRequest:
    * sends the read request to the Partition (TEvReadBlocksLocalRequest) for a specified blocks.
      * TPartitionActor::HandleReadBlocksLocal -> HandleReadBlocksRequest:
        * reading blocks request is created;
        * TReadBlocksLocalHandler is registered through CreateReadHandler for the subsequent response's returning
        * blocks are read via the CreateTx<TReadBlocks> transaction with the TReadBlocksLocalHandler handler with start in the TPartitionActor::ReadBlocks;
        * TPartitionActor::CompleteReadBlocks is called at the end of the transaction:
          * the TReadBlocksActor actor is registered;
            * the work will be done with TReadBlocksLocalHandler::GetGuardedSgList, called from TReadBlocksActor::ReadBlocks;
            * TEvReadBlobRequest is sent;
            * TReadBlocksActor::HandleReadBlobResponse:
              * check checksums in VerifyChecksums for each response;
              * wait for all responses;
              * generate the response through CreateReadBlocksResponse;
    * errors are checked and the received checksums are calculated and set in response processing (TCheckRangeActor::HandleReadBlocksResponse)

## Unit tests
### Volume
It contains common logic for all types of partitioning. The disk type is received as a parameter.

#### Positive tests:
- DoTestShouldCommonCheckRange: getting one general response for checkRange + checksum check;
- DoTestShouldSuccessfullyCheckRangeIfDiskIsEmpty: checkRange check for an empty disk;
- DoTestShouldGetSameChecksumsWhileCheckRangeEqualBlocks: we compare checksums of the same data from different blocks;
- DoTEstShouldGetDifferentChecksumsWhileCheckRange: we check the difference in checksums of different blocks;
- DoTestShouldCheckRangeIndependentChecksum: we check the independence of checksums of adjacent blocks.

#### Negative tests:
- DoTestShouldCheckRangeWithBrokenBlocks: we check the «block is broken» error by substituting the intercepted internal response;
- DoTestShouldntCheckRangeWithBigBlockCount: we check the processing of a parameter for a block size that is too large.

### Nonreplicated disk
All tests are implemented as «common» in volume.

### ydb-based disk
All tests are implemented as «common» in volume.

### Mirror disk
Many tests are implemented as «common» in volume.
#### Positive tests:
- ShouldMirrorCheckRangeRepliesFromAllReplicas: receiving intermediate responses from each of the 3 replicas. This is necessary to check the participation of each replica;

#### Negative tests:
- ShouldMirrorCheckRangeOneReplicaBroken: checking the «block is broken» error on 1 replica by substituting the intercepted internal response;
- ShouldMirrorCheckRangeOneReplicaDifferent: checking the case of a checksum's difference on a 1 replica.
