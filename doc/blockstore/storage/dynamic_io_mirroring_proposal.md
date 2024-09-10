# Dynamic mirroring of IO requests in mirror-[2,3] disks proposal

## Problem

Right now, mirror disks work on top of nonreplicated partitions. Write requests are simply copied for each replica. Read requests performed on one of the replicas chosen by round-robin.
If one of the agents becomes unavailable due to e.g. host or network malfunction, all writes are blocked and the bandwidth of reads is reduced by several orders of magnitude. IO unlocks either because the agent became online again or the DiskRegistry declared the host unavailable and reallocated the volume (with the subsequent start of the migration).

## Overview

The reads issue can be solved easily by removing the unavailable agent from the round-robin. Writes, on the other hand, are a little bit trickier. Our suggestion — is to continue to write on two good replicas and store a dirty blocks map while the agent is unavailable. If it becomes online after just a small delay, we will migrate dirty blocks from the good replicas.

Since we deal with nonreplicated disks, we can't afford to store the map in the persistent DB. We will store the fact (and indexes of the lagging agents) that a disk is not mirroring its IO to all replicas. If this flag is set at the start of the service, the resync process will be started. The `VolumeActor` will store the data and decide whether it's possible to ignore a bad replica.

`NonreplicatedPartition` is already tracking the response times of devices. An agent will be declared as unavailable once one of its devices has not responded for some time. The plan right now is to start from 5 seconds and slowly bring it down to values that are unnoticeable to users.

Note that we make an assumption, that if one of the agent devices becomes unresponsive, it probably means that the other are too. There is no such guarantee, of course. Technically long response times could indicate a faulty SSD. But in that case, after volume reallocation lagging behind devices will resync.
The major benefit of such an assumption is that when all of the replica devices of an agent belong to a single SSD, user IO will hiccup only once.

## Goals

Ultimately, we wish that the user will not notice a situation when one of the devices per row is unavailable.
Imagine Mirror-3 disk with 5 devices. The disk devices belong to 8 different agents. Agents 1, 5, and 7 are unavailable, agent 4 has become online after a short period of network issues. The rest are ok.

| Replica 0    | Replica 1   | Replica 2 |
| ------------ | ----------- | --------- |
| Agent-1 X    | Agent-3 ✓   | Agent-6 ✓ |
| Agent-2 ✓    | Agent-3 ✓   | Agent-7 X |
| Agent-2 ✓    | Agent-4 ↻   | Agent-8 ✓ |
| Agent-2 ✓    | Agent-4 ↻   | Agent-8 ✓ |
| Agent-2 ✓    | Agent-5 X   | Agent-8 ✓ |

In this state, no matter the block index, a write operation will be performed on two good devices. This is sufficient for us to guarantee data safety.

## Detailed Design

There wiil be 3 new entities:
1) `IncompleteMirrorRWModeController`
2) `AgentAvailabilityMonitor`
3) `SmartMigrationActor`

In the example above, architecture schema will look like this:

```mermaid
flowchart TD
    A[Volume]  --> B[MirrorPartition]
    B --> C[IncompleteMirrorRWModeController]
    B --> D[IncompleteMirrorRWModeController]
    B --> E[IncompleteMirrorRWModeController]
    C --> F[AgentAvailabilityMonitor agent-1]
    C --> G[NonreplicatedPartition]
    F --> G
    D --> H[AgentAvailabilityMonitor agent-5]
    D --> I[SmartMigrationActor agent-4]
    D --> J[NonreplicatedPartition]
    H --> J
    I --> J
    E --> K[AgentAvailabilityMonitor agent-7]
    K --> L[NonreplicatedPartition]
    E --> L
```

### IncompleteMirrorRWModeController

This actor proxies all IO messages between MirrorPartition and `NonreplicatedPartition`. Its purpose to manage lagging agents in one of the replicas.
A lagging agent can be either unresponsive or resyncing.

- In the unresponsive state:
    - `AgentAvailabilityMonitor` is created.
    - Doesn't accept reads.
    - Writes that hit unavailable agent instantly replied with `S_OK` and their range is stored in the dirty block map.
    - Writes that hit 2 agents and one of them is available are split into two parts. The range of the unavailable one is stored in the map. The second one is proxied to `NonreplicatedPartition`.
    - Writes that hit available agents are just proxied to the `NonreplicatedPartition`.
    - Ultimately, waiting for one of the two events: the volume reallocates and creates a migration partition, or `AgentAvailabilityMonitor` notifies that the agent has become available. The second event switches state to the resyncing.

- In the resyncing state:
    - `SmartMigrationActor` is created.
    - Doesn't accept reads.
    - Writes are proxied to `SmartMigrationActor`.

There can be 0-1 instances of `IncompleteMirrorRWModeController` per `NonreplicatedPartition`. The presence of the `IncompleteMirrorRWModeController` indicates that the replica has agents that lag behind. `IncompleteMirrorRWModeController` manages the lifetimes of `AgentAvailabilityMonitor` and `SmartMigrationActor` entities.

Since the dirty block map will not be stored persistently, we must handle lagging replica on restart of a partition, volume, or a whole service. In this case, the basic resync is started, but with a small difference that only devices of lagging agents will be processed.
There is one caveat, though: mirror-3 disks can now store different data in the same block across all three replicas. The lagging replica - the oldest data and the other two can differ because a write blocks request was sent to only one replica before the restart. That is not a problem because write confirmation was not sent to a client, but it is something that the current resync algorithm is not ready for.

### AgentAvailabilityMonitor

This is simple actor that periodically reads a block with a small timeout. Once it is succeded, it notifies the `IncompleteMirrorRWModeController` which in response will destroy the `AgentAvailabilityMonitor` and create a `SmartMigrationActor`.

### SmartMigrationActor

`SmartMigrationActor` is an actor that migrates dirty block map (collected by `IncompleteMirrorRWModeController`) from 2 available agents to the lagging one. It inherits from `TNonreplicatedPartitionMigrationCommonActor` since it already has most of the necessary functionality:
- Copies from one actor to the other
- Does copying by blocks map
- Proxies user writes to the `NonreplicatedPartition`
- Prioritizes migration over user writes

### Sequence diagram

Example of a sequence diagram when one of the devices becomes unresponsive:
```mermaid
sequenceDiagram
    participant Volume
    participant MirrorPartition
    participant IncompleteMirrorRWModeController
    participant AgentAvailabilityMonitor
    participant SmartMigrationActor
    participant NonreplicatedPartition

    NonreplicatedPartition ->> Volume: Device is unresponsive
    Volume ->> Volume: Decide whether we can avoid IO to the unresponsive agent
    Volume ->> Volume: If ok, save unresponsive agent info persistently
    Volume ->> MirrorPartition: Disable reads and writes to the unresponsive agent
    MirrorPartition ->> IncompleteMirrorRWModeController: Ensure created
    IncompleteMirrorRWModeController ->> AgentAvailabilityMonitor: Create
    IncompleteMirrorRWModeController ->> NonreplicatedPartition: Reject pending requests
    AgentAvailabilityMonitor ->> NonreplicatedPartition: Wait until the agent becomes available
    NonreplicatedPartition ->> AgentAvailabilityMonitor: Agent has responded on read request
    AgentAvailabilityMonitor ->> IncompleteMirrorRWModeController: Report the agent is available
    IncompleteMirrorRWModeController -x AgentAvailabilityMonitor: Destroy
    IncompleteMirrorRWModeController ->> MirrorPartition: Enable writes to the lagging agent
    IncompleteMirrorRWModeController ->> SmartMigrationActor: Create
    SmartMigrationActor ->> SmartMigrationActor: Migrate lagging blocks
    SmartMigrationActor ->> IncompleteMirrorRWModeController: Finished migration
    IncompleteMirrorRWModeController ->> Volume: Replicas are in sync
    Volume ->> Volume: Delete unresponsive agent info from DB
    Volume ->> MirrorPartition: Enable reads to the lagging agent
    MirrorPartition -x IncompleteMirrorRWModeController: Destroy
```
