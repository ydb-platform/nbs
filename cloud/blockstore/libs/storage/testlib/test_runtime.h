#pragma once

#include <cloud/blockstore/config/features.pb.h>

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <ydb/core/base/tabletid.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

const ui64 HiveId = NKikimr::MakeDefaultHiveID(0);

const ui64 TestTabletId  = NKikimr::MakeTabletID(0, HiveId, 1);
const ui64 TestTabletId2 = NKikimr::MakeTabletID(0, HiveId, 2);

////////////////////////////////////////////////////////////////////////////////

inline NActors::TActorId Register(
    NActors::TTestActorRuntime& runtime,
    NActors::IActorPtr actor)
{
    return runtime.Register(actor.release());
}

inline void Send(
    NActors::TTestActorRuntime& runtime,
    const NActors::TActorId& recipient,
    const NActors::TActorId& sender,
    NActors::IEventBasePtr event,
    ui32 nodeIdx = 0)
{
    runtime.Send(new NActors::IEventHandle(recipient, sender, event.release()), nodeIdx);
}

inline void SendToPipe(
    NActors::TTestActorRuntime& runtime,
    ui64 tabletId,
    const NActors::TActorId& sender,
    NActors::IEventBasePtr event,
    ui32 nodeIdx = 0)
{
    runtime.SendToPipe(
        tabletId,
        sender,
        event.release(),
        nodeIdx,
        NKikimr::GetPipeConfigWithRetries());
}

template <typename T>
inline bool Succeeded(T* response)
{
    return response && SUCCEEDED(response->GetStatus());
}

template <typename T>
inline TString GetErrorReason(T* response)
{
    if (!response) {
        return "<null response>";
    }
    if (!response->GetErrorReason()) {
        return "<no error reason>";
    }
    return response->GetErrorReason();
}

TStorageConfigPtr CreateTestStorageConfig(
    NProto::TStorageServiceConfig config,
    NProto::TFeaturesConfig featuresConfig = {});

}   // namespace NCloud::NBlockStore::NStorage
