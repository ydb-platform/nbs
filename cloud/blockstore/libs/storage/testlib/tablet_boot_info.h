#pragma once

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Compares reports with the storage metadata used by the test bootstrapper and
// the generations confirmed by the tablet runtime, including after a reboot.
class TTabletBootInfoObserver
{
private:
    NActors::TTestActorRuntimeBase& Runtime;
    NActors::TTestActorRuntimeBase::TEventFilter PreviousEventFilter;
    const ui64 ExpectedTabletId;
    TString ExpectedStorageInfo;
    THashMap<NActors::TActorId, ui32> RestoredGenerations;
    THashMap<NActors::TActorId, ui32> ReportedGenerations;
    ui32 LastGeneration = 0;

public:
    TTabletBootInfoObserver(
        NActors::TTestActorRuntimeBase& runtime,
        const NKikimr::TTabletStorageInfo& expectedStorageInfo)
        : Runtime(runtime)
        , ExpectedTabletId(expectedStorageInfo.TabletID)
    {
        NKikimrTabletBase::TTabletStorageInfo proto;
        NKikimr::TabletStorageInfoToProto(expectedStorageInfo, &proto);
        ExpectedStorageInfo = proto.DebugString();

        PreviousEventFilter = Runtime.SetEventFilter(
            [this](auto& runtime, auto& event)
            {
                Observe(*event);
                return PreviousEventFilter(runtime, event);
            });
        if (!PreviousEventFilter) {
            PreviousEventFilter =
                NActors::TTestActorRuntimeBase::DefaultFilterFunc;
        }
    }

    TTabletBootInfoObserver(const TTabletBootInfoObserver&) = delete;
    TTabletBootInfoObserver& operator=(const TTabletBootInfoObserver&) = delete;
    TTabletBootInfoObserver(TTabletBootInfoObserver&&) = delete;
    TTabletBootInfoObserver& operator=(TTabletBootInfoObserver&&) = delete;

    ~TTabletBootInfoObserver()
    {
        Runtime.SetEventFilter(std::move(PreviousEventFilter));
    }

    size_t GetBootCount() const
    {
        return RestoredGenerations.size();
    }

    size_t GetReportCount() const
    {
        return ReportedGenerations.size();
    }

    ui32 GetLastGeneration() const
    {
        return LastGeneration;
    }

    void CheckReports() const
    {
        UNIT_ASSERT(!ReportedGenerations.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            RestoredGenerations.size(),
            ReportedGenerations.size());
        for (const auto& [actorId, generation]: ReportedGenerations) {
            const auto it = RestoredGenerations.find(actorId);
            UNIT_ASSERT(it != RestoredGenerations.end());
            UNIT_ASSERT_VALUES_EQUAL(generation, it->second);
        }
    }

private:
    void Observe(NActors::IEventHandle& event)
    {
        using namespace NCloud::NStorage;

        switch (event.GetTypeRewrite()) {
            case NKikimr::TEvTablet::EvRestored: {
                const auto* msg = event.Get<NKikimr::TEvTablet::TEvRestored>();
                if (msg->TabletID == ExpectedTabletId && !msg->Follower) {
                    RestoredGenerations[msg->UserTabletActor] = msg->Generation;
                }
                break;
            }
            case TEvHiveProxy::EvUpdateTabletBootInfoBackup: {
                const auto* msg =
                    event.Get<TEvHiveProxy::TEvUpdateTabletBootInfoBackup>();
                UNIT_ASSERT(msg->StorageInfo);
                if (msg->StorageInfo->TabletID != ExpectedTabletId) {
                    break;
                }

                UNIT_ASSERT_VALUES_EQUAL(
                    event.Recipient,
                    MakeHiveProxyServiceId());
                NKikimrTabletBase::TTabletStorageInfo proto;
                NKikimr::TabletStorageInfoToProto(*msg->StorageInfo, &proto);
                UNIT_ASSERT_VALUES_EQUAL(
                    ExpectedStorageInfo,
                    proto.DebugString());
                UNIT_ASSERT(
                    ReportedGenerations.emplace(event.Sender, msg->Generation)
                        .second);
                LastGeneration = msg->Generation;
                break;
            }
        }
    }
};

}   // namespace NCloud::NBlockStore::NStorage
