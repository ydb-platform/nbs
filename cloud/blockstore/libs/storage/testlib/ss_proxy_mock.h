#pragma once

#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>

#include <contrib/ydb/core/protos/subdomains.pb.h>
#include <contrib/ydb/core/protos/bind_channel_storage_pool.pb.h>

#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////


class TSSProxyMock final
    : public NActors::TActor<TSSProxyMock>
{
public:
    struct TPoolDescr
    {
        TString Name;
        TString Kind;
    };

    struct TVolumeTabletDescr
    {
        TString DiskId;
        ui64 TabletId;
    };

private:
    THashMap<TString, NKikimrBlockStore::TVolumeConfig> Volumes;
    TVector<TPoolDescr> Pools;
    TVector<TVolumeTabletDescr> VolumeTablets;

public:
    TSSProxyMock()
        : TActor(&TThis::StateWork)
    {}

    explicit TSSProxyMock(TVector<TPoolDescr> pools)
        : TActor(&TThis::StateWork)
        , Pools(std::move(pools))
    {}

    explicit TSSProxyMock(TVector<TVolumeTabletDescr> volumeTablets)
        : TActor(&TThis::StateWork)
        , VolumeTablets(std::move(volumeTablets))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSSProxy::TEvCreateVolumeRequest, HandleCreateVolume);
            HFunc(TEvSSProxy::TEvModifyVolumeRequest, HandleDestroyVolume);
            HFunc(TEvSSProxy::TEvDescribeSchemeRequest, HandleListVolumes);
            HFunc(TEvSSProxy::TEvDescribeVolumeRequest, HandleDescribeVolume);
            HFunc(TEvSSProxy::TEvModifySchemeRequest, HandleModifyScheme);
        }
    }

    void HandleCreateVolume(
        const TEvSSProxy::TEvCreateVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto& diskId = msg->VolumeConfig.GetDiskId();

        Y_ABORT_UNLESS(!Volumes.contains(diskId));

        Volumes[diskId] = msg->VolumeConfig;

        auto response = std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDestroyVolume(
        const TEvSSProxy::TEvModifyVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        Y_ABORT_UNLESS(msg->OpType ==
            TEvSSProxy::TModifyVolumeRequest::EOpType::Destroy);

        Y_ABORT_UNLESS(Volumes.contains(msg->DiskId));
        Volumes.erase(msg->DiskId);

        auto response = std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDescribeVolume(
        const TEvSSProxy::TEvDescribeVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        auto it = Volumes.find(msg->DiskId);

        if (it == Volumes.end()) {
            auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                MakeError(
                    MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist)
                )
            );

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        NKikimrSchemeOp::TPathDescription p;
        auto* descr = p.MutableBlockStoreVolumeDescription();
        descr->SetName(msg->DiskId);
        descr->MutableVolumeConfig()->CopyFrom(it->second);

        for (const auto& volumeTablet: VolumeTablets) {
            if (volumeTablet.DiskId == msg->DiskId) {
                descr->SetVolumeTabletId(volumeTablet.TabletId);
            }
        }

        auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
            msg->DiskId,
            std::move(p));

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleListVolumes(
        const TEvSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        NKikimrSchemeOp::TPathDescription p;
        auto& self = *p.MutableSelf();

        TStringBuf path = msg->Path;

        if (path == "/Root") {
            self.SetPathType(NKikimrSchemeOp::EPathTypeDir);

            if (!Pools.empty()) {
                auto& domain = *p.MutableDomainDescription();
                for (const auto& pool: Pools) {
                    auto& p = *domain.AddStoragePools();
                    p.SetName(pool.Name);
                    p.SetKind(pool.Kind);
                }
            }

            for (const auto& [diskId, config]: Volumes) {
                Y_UNUSED(config);

                auto& c = *p.AddChildren();
                c.SetName(diskId);
                c.SetPathType(NKikimrSchemeOp::EPathTypeBlockStoreVolume);
            }
        } else {
            Y_ABORT_UNLESS(path.SkipPrefix("/Root/"));
            Y_ABORT_UNLESS(Volumes.contains(path));

            self.SetPathType(NKikimrSchemeOp::EPathTypeBlockStoreVolume);
            self.SetName(ToString(path));
            self.SetCreateFinished(true);

            auto& volumeDescr = *p.MutableBlockStoreVolumeDescription();
            volumeDescr.SetName(ToString(path));

            auto& config = *volumeDescr.MutableVolumeConfig();
            if (path.StartsWith("nonrepl-")
                    || path.StartsWith("hdd-nonrepl-")
                    || path.StartsWith("mirror"))
            {
                config.SetStorageMediaKind(path.StartsWith("nonrepl-")
                    ? NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
                    : path.StartsWith("hdd-nonrepl-")
                    ? NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED
                    : path.StartsWith("mirror2-")
                    ? NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2
                    : NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);
                config.SetBlockSize(4096);
                auto& partition = *config.AddPartitions();
                partition.SetBlockCount(1000000);
                partition.SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
            } else {
                config.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
            }
        }

        auto response = std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
            msg->Path, std::move(p));

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleModifyScheme(
        const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<TEvSSProxy::TEvModifySchemeResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }
};

}   // namespace NCloud::NBlockStore::NStorage
