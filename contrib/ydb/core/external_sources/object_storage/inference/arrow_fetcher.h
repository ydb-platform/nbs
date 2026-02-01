#pragma once

#include <contrib/ydb/core/external_sources/object_storage/inference/arrow_inferencinator.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

NActors::IActor* CreateArrowFetchingActor(NActors::TActorId s3FetcherId, EFileFormat format);
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
