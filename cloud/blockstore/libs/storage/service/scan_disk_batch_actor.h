#pragma once

#include "public.h"

#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Lists a batch of blobs of one partition, checks them and confirms the
// unhealthy verdicts. Replies to the owner with
// TEvServicePrivate::TEvScanDiskBatchResponse and keeps nothing afterwards.
NActors::IActorPtr CreateScanDiskBatchActor(
    NActors::TActorId owner,
    NPrivateProto::TScanDiskBatchRequest request);

}   // namespace NCloud::NBlockStore::NStorage
