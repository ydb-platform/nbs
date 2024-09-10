# How session management works during qemu migrations

## Problem

Compute should be able to migrate qemu instances accross cloud transparently for the user. These online qemu migrations require simultaneous access from source (old qemu instance) and target (new qemu intance). So we need a way to support multiple opened endpoints per one client instance and protocol for switching write access from one to another.

## Sessions

In NBS and Filestore all communications with the service are done within sessions, so as a first step every client has to establish one. In turn, it helps to prevent any stale clients from corrupting any data by rejecting their rpcs. For example disk allows only one write session at a time. So when client establishes new write session with a disk it in turn kicks out another client write session (if any). Thus, it rejects any rpc calls which weren't originated by current rw session.

Filestore sessions between qemu instances and filesystems are stateful unlike disk sessions. Session state consists of created file handles which in turn keeps file locks and unlinked inodes. Thus, in case of instance migration it is a must to restore actual client session instead of creating a new one.

## Migration kinds

 - Local - put it simple qemu process is being restarted while opened endpoint is being kept intact. Thus, nothing is required from session management point of view.
 - Online Remote - bringing up new qemu instance (target) on another host, syncing state and switching over.
 - Online Local - same as remote but on the same host which requires more careful handling of items in local services.

## General migration steps

Major steps during migration:
1. Starting new endpoint with the same client id and bigger seqno with RO access.
2. Waiting for qemu to finish sync.
3. Starting endpoint with the same client and seqno as in #1 but with RW access.
4. Continuing qemu process on the target host.
5. Stopping endpoint on the source host.

## Solution

1. Allowing multiple RO clients and one RW client per session in the tablet:
    - track actor ids per session and their access roles;
    - validate current role of an actor upon create/destroy requests;
    - leaving session intact upon RO clients destroy requests if there is an active RW client;

2. Tracking seqno within request headers:
    - using seqno to distinguish between different clients modifying one session;
    - using seqno within session to validate current epoch of the requests;

3. Support for session remounting:
    - need to support either explicit call or use create session call but with new access mode in tablet;
    - support to start endpoint requests comparison and promoting sessions when necessary;

4. Seqno should strongly increase for each endpoint start:
    - it is enough to track seqno per each instance like it's done for the disks;
    - it is ok to use the same counter for all filestores within one instance;
    - it is ok to share counter per instance accross all endpoints including disks (optionally);

## Components changes

### Endpoint manager
[code](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/endpoint/service.cpp#L101)

    Manages existing opened endpoints by socket path:
    - should explicitly create and track sessions;
    - should compare start endpoint request against currently opened and promote session when neccessary;

### Endpoint
[code](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/endpoint_vhost/listener.cpp#L87)

    Connects filesystem, driver and session:
    - should use existing session for starting endpoint;

### Storage Service
[code](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/service/service_actor.h#L32)

    Terminates public session api and switches to actor/pipe world. Keeps actor & pipe per each opened session:
    - should use seqno to distinguish different sessions with the same id;
    - should accordingly distinguish create/destroy sessions for sessions with the same id;

### Tablet
[code](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/tablet_actor.h#L46)

    Persists session state, owners and keep tracks of its owners:
    - should be able to keep track of multiple session owners and their access state;
    - should be able to promote/demote RW access within one session;
    - should properly handle create/destroy session requests according to owner access rights;
    - should check validity for session seqno for the requests;

### Client
[code](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/client/session.h#L61)

    Manages session for the client:
    - should be able to promote session RW access rights;
    - should add seqno to requests headers for every request;
