# Shard sessions sync: session requests in batches

Status: proposal.

## Problem

Each mount (VM) creates a session in the main tablet. The main tablet creates
the session in each of the shards and responds only after all the shards have
responded (see "CRUD and Session Management" in [sharding.md](sharding.md)).
`ResetSession` (vhost sends it upon FUSE init and destroy) and
`DestroySession` are sent to all the shards the same way.

So a mount costs two requests per shard (CreateSession and DestroySession).
If 1000 VMs start simultaneously and the filesystem has 25K shards, the main
tablet sends 50M requests to the shards at once. It gets overloaded, the
requests time out and the retries add more load.

## Idea

The main tablet remains the only owner of the sessions, the shards keep the
copies. A request per session per shard is replaced with the shard sessions
sync: a single place which sends one batched request per shard with
everything that all the clients are waiting for. What the clients see doesn't
change: a session request is acknowledged only after all the shards have
processed it.

1. The sync works in rounds. A round sends one `SyncShardSessions` request
   per shard at most and processes a limited number of shards at a time.
2. `CreateSession`, `ResetSession` and `DestroySession` become pending in the
   main tablet and start a round right away. The client gets the response
   when all the shards have processed the request. Any number of simultaneous
   requests share the same round.
3. Everything that the shards should get has a version. The main tablet
   remembers which version each shard has been synced at and sends only what
   is newer. A regular round (the existing schedule, `IdleSessionTimeout / 3`)
   asks each shard for its sessions and fixes any discrepancy.
4. A request has no timeout: it waits for as long as some of the shards are
   unavailable. Request is completed as soon as each shard has been processed
   it.
5. The requests are idempotent: a client may retry a request at any moment.
   A retry is recognized by the request id.
6. An independent improvement: a shard which has no active session for a
   request of a client (e.g. after its restart) requests the session from
   the main tablet instead of failing the request. This is called on-demand
   shard session recovery.

| 1000 simultaneous mounts, 25K shards | Current | Proposed |
| --- | --- | --- |
| requests to shards | 25M `CreateSession` + 25M `ResetSession` | a few rounds, 25K small requests each at most |
| duration of a mount | two fan-outs to all the shards | at least two rounds |

The regular rounds stay expensive (a shard returns all its sessions), but
they run once in 20 minutes by default, not upon each change.

```mermaid
flowchart LR
    Client["Client<br/>(a VM mount served by vhost)"]
    Main["Main tablet<br/>owns the sessions,<br/>runs the shard sessions sync"]
    Shard["Shard tablets<br/>keep the copies of the sessions"]
    Fast["Shards in adapter mode<br/>(fast shards)"]

    Client -->|"CreateSession<br/>ResetSession<br/>DestroySession"| Main
    Client -->|"requests for the nodes<br/>of the shard"| Shard
    Main -->|"shard sessions sync:<br/>DescribeShardSessions<br/>SyncShardSessions"| Shard
    Shard -->|"on demand:<br/>RecoverShardSessions"| Main
    Main -->|"the old way:<br/>per-session requests"| Fast
```

The shards which work in adapter mode can't process the requests of the
sync: their sessions are still managed by the per-session requests.

## Session requests

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (vhost)
    participant M as Main tablet
    participant A as Shard sessions sync
    participant S as Shards 1..M

    C->>M: CreateSession
    M->>M: Tx_CreateSession (creates or recovers the session)
    M->>A: keep the response, start a round
    A->>S: SyncShardSessions (the session, Interrupted if it has been recovered)
    S->>S: create the session or recover the copy, drop its unconfirmed writes if Interrupted
    S-->>A: response
    A->>M: all the shards have been synced
    M-->>C: CreateSessionResponse

    C->>M: ResetSession / DestroySession (SessionSeqNo = N)
    M->>A: keep the request, don't touch the session, start a round
    A->>S: SyncShardSessions (the request + the session the way the main tablet sees it)
    S->>S: make the subsessions the same, Tx_ResetSession / Tx_DestroySession (the regular check of N)
    S-->>A: response
    A->>M: all the shards have been synced
    M->>M: Tx_ResetSession / Tx_DestroySession
    M-->>C: response

    Note over M,S: A round has failed for some of the shards: the request stays pending,<br/>the next round delivers it to them. There is no timeout:<br/>the request waits for as long as a shard is unavailable.<br/>A retry (the same RequestId) takes the place of the request and keeps what has been done
```

* **CreateSession.** The transaction of the main tablet creates or recovers
  the session, the response waits until the round delivers the session to
  all the shards: a shard creates the session or makes its copy active with
  the same subsessions. If the transaction has recovered an existing session
  and the request is not a retry (the client has started anew: vhost has
  been restarted, a VM is being migrated), the session is marked as
  `Interrupted` and the shard drops its unconfirmed writes before it
  responds - the same thing a tablet does when it recovers a session upon
  `CreateSession` itself. One more `CreateSession` for the same session takes
  the place of the pending one.
* **Why CreateSession is not lazy.** Replying before the shards have got the
  session would save a round per mount. But a client which recovers its
  session repeats the requests which its previous incarnation hasn't
  completed and goes on, while a shard may still keep an unconfirmed write of
  that incarnation: the blobs are written, the write is not confirmed. A
  shard which gets restarted applies all the unconfirmed writes which it
  finds, so that write would be applied over the data which the new
  incarnation has written to the same range later - an acknowledged write
  would be lost. So each shard has to drop these writes before the client
  gets the session back, and only the main tablet knows that the session has
  been recovered. A write of a client which is still there is not lost: its
  confirmation fails and it falls back to a regular write.

## Sync rounds

```mermaid
sequenceDiagram
    autonumber
    participant M as Main tablet
    participant A as TBatchedSyncShardSessionsActor
    participant S as Shard tablet

    M->>A: start a round at version W
    loop for each shard, not more than MaxShardSessionsSyncInflight shards at a time
        alt a regular round, an unknown shard or a restarted shard
            A->>S: DescribeShardSessions
            S-->>A: its sessions, Generation
            A->>M: BuildShardSessionsDiff (the sessions of the shard)
            M-->>A: what differs + the pending requests which the shard hasn't processed
        else a round which is started by a request, the shard has been synced at version V
            A->>M: BuildShardSessionsDiff (ChangesOnly)
            M-->>A: the sessions and the pending requests with versions after V and up to W
        end
        opt there is something to send
            A->>S: SyncShardSessions (a single request)
            S-->>A: Generation
        end
    end
    A->>M: SyncSessionsCompleted (the synced shards and their generations)
    M->>M: remember version W and the generation of each synced shard
    M->>M: complete the pending requests which all the shards have processed
```

* **When a round is started.** By the regular schedule; right away when a
  request becomes pending; `ShardSessionsSyncDelay` after a round which
  hasn't delivered a pending request to some of the shards or after a request
  has released its session and has left it alive. Only one round runs at a
  time.
* **What a shard gets from a round which is started by a request.** The
  sessions and the pending requests which are newer than the version of the
  shard. The main tablet doesn't know which shards have a session, so each
  shard gets each request: `ResetSession` / `DestroySession` for a session
  which the shard doesn't have do nothing there. A shard which has nothing
  to get is not contacted at all.
* **When a shard is compared in full.** By each regular round. By any round
  if the main tablet knows nothing about the shard (the main tablet has been
  restarted) or if the shard has been restarted - the generation in its
  response is not the remembered one, and none of its sessions is active
  until the main tablet recovers them. The comparison sends the sessions
  which are missing, not active or have other subsessions, and destroys the
  sessions which are unknown to the main tablet.

## Session recovery on demand

A session is in all the shards by the time its client gets it, so a shard
lacks an active session only if something has gone wrong - usually the shard
has been restarted. Without this mechanism the client gets
`E_FS_INVALID_SESSION` and establishes the session anew. With it the shard
postpones the request, asks the main tablet for the session
(`RecoverShardSessions`, one request per session), recovers it and processes
the postponed requests in the order of their arrival. If the session is not
active in the main tablet, the requests fail with `E_FS_INVALID_SESSION`; if
it's being destroyed or the main tablet is unavailable - with `E_REJECTED`.
