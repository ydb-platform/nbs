#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include "part_nonrepl_events_private.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ProcessMirrorActorError(NProto::TError& error);

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<NActors::TActorId> replicaIds,
    NProto::TWriteBlocksRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

NActors::IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<NActors::TActorId> replicaIds,
    NProto::TWriteBlocksLocalRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

NActors::IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<NActors::TActorId> replicaIds,
    NProto::TZeroBlocksRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId,
    NProto::TWriteBlocksRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

NActors::IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId,
    NProto::TWriteBlocksLocalRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

NActors::IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId,
    NProto::TZeroBlocksRequest request,
    TString diskId,
    NActors::TActorId parentActorId,
    ui64 nonreplicatedRequestCounter);

}   // namespace NCloud::NBlockStore::NStorage
