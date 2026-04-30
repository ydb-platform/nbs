# Volume Balancer

## Reworked volume preemption mechanism

New volume preemption is intended to prevent volume balancer from breaking
well-mounted disks during Hive downtime

Original implementation of ChangeVolumeBinding uses unsafe local-to-remote
remount algorithm

1. Stop volume tablet
2. Release tablet lock in Hive
3. Wait for Hive to boot new tablet
   which kills local volume tablet before knowing the Hive is able to boot it
   remotely

```mermaid
sequenceDiagram
    box Local AS
        participant VolumeBalancer
        participant Service
        participant VolumeSession
        participant MountRequestActor
        participant AddClientActor
        participant StartVolumeActor
        participant Volume
        participant WaitReadyActor
    end
    box Remote AS
        participant Volume_Remote
    end
    box Sys Tablets
        participant Hive
        participant SS
    end

%%  Initial state
    activate Hive
    activate SS
    activate VolumeBalancer
    activate Service
    activate VolumeSession
    activate Volume
    activate StartVolumeActor
%%  Initial state
    VolumeBalancer ->> Service: TEvChangeVolumeBindingRequest
    Service ->> VolumeSession: TEvChangeVolumeBindingRequest
    VolumeSession ->>+ MountRequestActor: Register Actor
    MountRequestActor ->> SS: TEvDescribeVolumeRequest
    SS -->> MountRequestActor: TEvDescribeVolumeResponse
    MountRequestActor ->>+ AddClientActor: Register Actor
    AddClientActor -->>- MountRequestActor: TEvAddClientResponse
    MountRequestActor ->> VolumeSession: TEvStopVolumeRequest
    VolumeSession ->> StartVolumeActor: TEvPoisonPill
    StartVolumeActor ->> Volume: TEvPoisonPill
    Volume ->>- StartVolumeActor: TEvTabletDead
    Note over Volume: Volume tablet is killed<br/>DPL is down
    StartVolumeActor ->> Hive: TEvUnlockTabletRequest
    Note over Hive: Hive dependency
    Hive -->> StartVolumeActor: TEvUnlockTabletResponse
    StartVolumeActor ->>- VolumeSession: TEvStartVolumeActorStopped
    VolumeSession -->> MountRequestActor: TEvStopVolumeResponse
    MountRequestActor ->>+ WaitReadyActor: Register Actor
    Note over Hive: Hive dependency
    Hive ->>+ Volume_Remote: Boot
    WaitReadyActor ->> Volume_Remote: Ready?
    Volume_Remote ->> WaitReadyActor: Ready
    WaitReadyActor -->>- MountRequestActor: TEvWaitReadyResponse
    MountRequestActor -->>- VolumeSession: TEvMountRequestProcessed
    VolumeSession -->> Service: TEvInternalMountVolumeResponse
    Service -->> Service: TEvChangeVolumeBindingResponse
    Service -->> VolumeBalancer: TEvChangeVolumeBindingResponse
%%  Final state
    deactivate Hive
    deactivate SS
    deactivate Volume_Remote
    deactivate VolumeBalancer
    deactivate Service
    deactivate VolumeSession
%%  Final state
```

New algorithm transfers the responsibility of local tablet killing to the Hive itself drastically reducing chances of
leaving volume in down state

```mermaid
sequenceDiagram
    box Local AS
        participant VolumeBalancer
        participant Service
        participant VolumeSession
        participant GentlePreemptionActor
        participant MountRequestActor
        participant AddClientActor
        participant StartVolumeActor
        participant Volume
        participant WaitReadyActor
    end
    box Remote AS
        participant Volume_Remote
    end
    box Sys Tablets
        participant Hive
        participant SS
    end

%%  Initial state
    activate Hive
    activate SS
    activate VolumeBalancer
    activate Service
    activate VolumeSession
    activate Volume
    activate StartVolumeActor
%%  Initial state
    VolumeBalancer ->> Service: TEvChangeVolumeBindingRequest
    Service ->> VolumeSession: TEvChangeVolumeBindingRequest
    VolumeSession ->>+ MountRequestActor: Register Actor
    MountRequestActor ->> SS: TEvDescribeVolumeRequest
    SS -->> MountRequestActor: TEvDescribeVolumeResponse
    MountRequestActor ->>+ AddClientActor: Register Actor
    AddClientActor -->>- MountRequestActor: TEvAddClientResponse
    MountRequestActor ->>+ GentlePreemptionActor: Register Actor
    GentlePreemptionActor ->> VolumeSession: TEvReleaseVolumeToHiveRequest
    VolumeSession ->> Hive: TEvUnlockTabletRequest
    Note over Hive: Hive dependency
    Hive -->> VolumeSession: TEvUnlockTabletResponse
    Note over Hive: Hive dependency
    Hive ->>+ Volume_Remote: Boot
    Volume_Remote ->> Volume: Demote
    Note over Volume: Local tablet is only demoted<br/>when the remote one is up
    Volume ->>- StartVolumeActor: TEvTabletDead
    StartVolumeActor ->> StartVolumeActor: Shutdown
    StartVolumeActor ->>- VolumeSession: TEvStartVolumeActorStopped
    VolumeSession -->> GentlePreemptionActor: TEvReleaseVolumeToHiveResponse
    GentlePreemptionActor -->>- MountRequestActor: TEvGentlePreemptionRequestProcessed
    MountRequestActor ->>+ WaitReadyActor: Register Actor
    WaitReadyActor ->> Volume_Remote: Ready?
    Volume_Remote ->> WaitReadyActor: Ready
    WaitReadyActor -->>- MountRequestActor: TEvWaitReadyResponse
    MountRequestActor -->>- VolumeSession: TEvMountRequestProcessed
    VolumeSession -->> Service: TEvInternalMountVolumeResponse
    Service -->> Service: TEvChangeVolumeBindingResponse
    Service -->> VolumeBalancer: TEvChangeVolumeBindingResponse
%%  Final state
    deactivate Hive
    deactivate SS
    deactivate Volume_Remote
    deactivate VolumeBalancer
    deactivate Service
    deactivate VolumeSession
%%  Final state
```

Remote-to-local migration already uses safe operation order and is not changed:

1. Lock volume tablet in hive
2. Boot volume tablet locally

Both migration operations are wrapped into timed retry-on-error loops inside of
GentlePreemptionActor,
which helps to contain inconsistencies inside a mount request and keep client
session up

Staying inside a mount request for a long time may by itself cause DPL lock
on TSession level due to regular remount being placed in queue.
This risk is already covered by introducing a non-blocking remount mode into
TSession (see `cloud/blockstore/config/client.proto::EnableNonBlockingRemount`).