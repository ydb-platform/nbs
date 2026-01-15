#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/*
                        Node 1                          |         Node 2
 ------------------------------------------------------ |-----------------------
               Client VM                                |
                   |                                    |
              Server (futures + promises)               |
                   |                                    |
                   v                                    |
      TKikimrService (TActorId + send)                  |
                   |                                    |
                   v                                    |
              TServiceActor-\                           |
                   |         |                          |
                   |         v                          |
                   |  VolumeSessionActor                |
                   |         |   |                      |
                   |         v   |                      |
                   |  VolumeClientActor <-------Pipe---------> |-------------|
                   |             v                      |      | VolumeActor |
                   |  VolumeClientActor <-------Pipe---------> |-------------|
                   |                                    |         ^     ^
                   |                                    |         |     |
           1      /|\         3                         |         |     |
    -------------/ | \-----------------                 |         |     |
    |              |                  |                 |         |     |
    |            2 |                  |                 |         |     |
    v              |                  |                 |         |     |
VolumeActor        v                  |                 |         |     |
             VolumeClientActor <---------------Pipe---------------/     |
                                      |                 |               |
                                      v                 |               |
                               VolumeProxy <---Pipe---------------------/
                                                        |
*/

NActors::IActorPtr CreateVolumeProxy(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerialize,
    bool temporaryServer);

}   // namespace NCloud::NBlockStore::NStorage
