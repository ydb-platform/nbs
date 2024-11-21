#pragma once

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <util/generic/string.h>

namespace NLastGetopt {
    class TOpts;
    class TOptsParseResultException;
}   // namespace NLastGetopt

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TVolumeId
{
private:
    TString DiskId;

public:
    TVolumeId(NLastGetopt::TOpts& opts);

public:
    template <class TRequest>
    void FillRequest(TRequest& request)
    {
        request.SetDiskId(DiskId);
    }

    explicit operator bool() const
    {
        return !!DiskId;
    }
};

class TVolumeParams
{
private:
    TString ProjectId;
    TString FolderId;
    TString CloudId;

public:
    TVolumeParams(NLastGetopt::TOpts& opts);

public:
    template <class TRequest>
    void FillRequest(TRequest& request)
    {
        request.SetProjectId(ProjectId);
        request.SetFolderId(FolderId);
        request.SetCloudId(CloudId);
    }
};

class TVolumeModelParams
{
private:
    ui64 BlocksCount = 0;

    ui64 PerformanceProfileMaxReadBandwidth = 0;
    ui64 PerformanceProfileMaxWriteBandwidth = 0;
    ui32 PerformanceProfileMaxReadIops = 0;
    ui32 PerformanceProfileMaxWriteIops = 0;
    ui32 PerformanceProfileBurstPercentage = 0;
    ui64 PerformanceProfileMaxPostponedWeight = 0;
    ui32 PerformanceProfileBoostTime = 0;
    ui32 PerformanceProfileBoostRefillTime = 0;
    ui32 PerformanceProfileBoostPercentage = 0;
    bool PerformanceProfileThrottlingEnabled = false;

public:
    TVolumeModelParams(NLastGetopt::TOpts& opts);

public:
    template <class TRequest>
    void FillRequest(TRequest& request)
    {
        request.SetBlocksCount(BlocksCount);

        auto pp = request.MutablePerformanceProfile();
        pp->SetMaxReadBandwidth(PerformanceProfileMaxReadBandwidth);
        pp->SetMaxWriteBandwidth(PerformanceProfileMaxWriteBandwidth);
        pp->SetMaxReadIops(PerformanceProfileMaxReadIops);
        pp->SetMaxWriteIops(PerformanceProfileMaxWriteIops);
        pp->SetBurstPercentage(PerformanceProfileBurstPercentage);
        pp->SetMaxPostponedWeight(PerformanceProfileMaxPostponedWeight);
        pp->SetBoostTime(PerformanceProfileBoostTime);
        pp->SetBoostRefillTime(PerformanceProfileBoostRefillTime);
        pp->SetBoostPercentage(PerformanceProfileBoostPercentage);
        pp->SetThrottlingEnabled(PerformanceProfileThrottlingEnabled);
    }
};

void ParseStorageMediaKind(
    const NLastGetopt::TOptsParseResultException& parseResult,
    const TString& storageMediaKindArg,
    NCloud::NProto::EStorageMediaKind& mediaKind);

}   // namespace NCloud::NBlockStore::NClient
