# io_uring transport: null-service benchmark configs (two hosts)

Methodology from the design spec: `nbsd --service null` with
`AddReadResponseData`, loadtest with 4 KiB x 64 blocks (256 KiB requests),
IoDepth 16, 60 s per run; A/B on one binary over `DirectSgIoEnabled`,
`SendZeroCopyMode`, `OobMode`/`OobThreshold`. Target: >= +60% IOPS over gRPC
(prototype: gRPC 6 096, inline 9 404, OOB 9 193, OOB+ZC 10 169).

Ports: **gRPC 9769** (control plane for both; data plane for the baseline),
**uring 9770** (data plane). nbsd and loadtest run on different hosts, so the
server binds `0.0.0.0` (the default `Host` is `localhost`).

## Prerequisite: stage 2 is not merged yet

Branch `io-uring-transport` is stage 1: the transport engine only. Nothing in
the daemon or in loadtest can use it today. The files marked *stage 2* below
follow the schema the design spec fixes for stage 2 and are the ones stage 2
must make real:

| file | schema | exists today |
|---|---|---|
| `nbsd/server.txt` | `TServerAppConfig` | yes (validated with protoc) |
| `nbsd/uring-*.txt` | `TUringConfig` (`cloud/blockstore/config/uring.proto`), `--uring-file` | **stage 2**; the `Server {}` body is `TUringServer` and validates today |
| `loadtest/client-grpc.txt` | `TClientConfig` | yes (validated) |
| `loadtest/client-uring-*.txt` | `TClientConfig` + `UringHost`, `UringPort`, `UringClientConfig` (`TUringClient`) | **stage 2**; `UringClientConfig` is an addition to the spec's loadtest section, needed for client-side A/B |
| `loadtest/workloads/*-grpc.txt` | loadtest graph | yes |
| `loadtest/workloads/*-uring.txt` | same + `IPC_URING = 6` in `EClientIpcType` | **stage 2** |

The gRPC baseline (`grpc` variant + `*-grpc.txt` workloads) can be run now.

## Host checklist (both hosts)

- `uname -r`: 5.15 baseline works without SEND_ZC; 6.0+ gives SEND_ZC,
  6.2+ reports `SendZcCopiedNotifications`. Record the kernel per host.
- `sysctl kernel.io_uring_disabled` must be 0; `ulimit -l` not tiny
  (ENOMEM/EPERM at start now throw instead of aborting; the log says why).
- `lldpd` or any packet socket on the host makes the kernel copy every ZC
  send silently; `SendZcCopiedNotifications` is the only signal. Check
  `ss -0` / `lsof -i` for raw sockets before measuring ZC.
- MTU, NIC offloads (TSO/GRO) and CPU frequency scaling identical between
  A and B runs; pin nbsd and loadtest away from the NIC IRQ cores.

## Run

nbsd host:

    MON_PORT=8766 nbsd/start-nbsd.sh <path>/nbsd grpc        # baseline
    MON_PORT=8766 nbsd/start-nbsd.sh <path>/nbsd baseline    # uring defaults
    MON_PORT=8766 nbsd/start-nbsd.sh <path>/nbsd zc-off | oob-off | inline-1m | staging

loadtest host (`<nbs-host>` = address of the nbsd host):

    loadtest/run.sh <path>/blockstore-loadtest <nbs-host> grpc  read-256k
    loadtest/run.sh <path>/blockstore-loadtest <nbs-host> uring read-256k baseline
    loadtest/run.sh <path>/blockstore-loadtest <nbs-host> uring read-256k zc-off

Workloads: `read-256k`, `write-256k`, `mixed-256k` (50/50), `read-4k`
(1 block, RPC overhead), `read-4k-256k` (uniform 1..64 blocks: small RPCs
behind large payloads, where OOB is supposed to pay off). Results land in
`results/<tag>.results.txt` (latency percentiles, requests completed; IOPS =
RequestsCompleted / 60) and `results/<tag>.log`.

Server and client variants must match for the negotiated knobs:
`inline-1m` on both sides (threshold = min), `oob-off` on either side is
enough, `zc-off` and `staging` are per side and can be mixed deliberately.

## Matrix (one row per 60 s run, repeat 3x)

| # | server | client | workload | question |
|---|---|---|---|---|
| 1 | grpc | grpc | read-256k | baseline |
| 2 | baseline | baseline | read-256k | OOB + ZC vs gRPC |
| 3 | inline-1m | inline-1m | read-256k | inline vs OOB on uniform load |
| 4 | zc-off | zc-off | read-256k | what ZC buys |
| 5 | staging | staging | read-256k | cost of the copy path |
| 6 | grpc / baseline | same | write-256k | write direction |
| 7 | grpc / baseline / inline-1m | same | read-4k-256k | OOB on mixed sizes |
| 8 | grpc / baseline | same | read-4k | pure RPC overhead |

## Counters to snapshot after each run

    collect-counters.sh <nbs-host> 8766 uring_server results/<tag>.server.json
    collect-counters.sh localhost  8767 uring_client results/<tag>.client.json

Look at `Requests`, `Errors`, `Rejected`, `Timeouts`, `InlineBytes` vs
`OobBytes` (confirms the mode actually negotiated), `DirectSendBytes` /
`DirectRecvBytes` vs `StagingCopyBytes` (confirms direct vs staging),
`SendZeroCopySupported`, `SendZcRequests` / `SendZcNotifications` /
`SendZcCopiedNotifications` (copied > 0 means the kernel fell back to a copy
and the ZC number is not a ZC number). Also take `top -H` of nbsd for the
`URING` ring threads vs the gRPC threads during the run.
