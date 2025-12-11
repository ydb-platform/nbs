#include "volume_manipulation_params.h"

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TVolumeId::TVolumeId(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("disk-id", "volume identifier")
        .RequiredArgument("STR")
        .StoreResult(&DiskId);
}

TVolumeParams::TVolumeParams(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("project-id", "owner information")
        .RequiredArgument("STR")
        .StoreResult(&ProjectId);

    opts.AddLongOption("folder-id", "user folder Id, used for billing")
        .RequiredArgument("STR")
        .StoreResult(&FolderId);

    opts.AddLongOption("cloud-id", "cloud Id, used for billing")
        .RequiredArgument("STR")
        .StoreResult(&CloudId);
}

TVolumeModelParams::TVolumeModelParams(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption(
            "blocks-count",
            "maximum number of blocks stored in volume")
        .RequiredArgument("NUM")
        .StoreResult(&BlocksCount);

    opts.AddLongOption(
            "performance-profile-max-read-bandwidth",
            "Volume performance profile max read bandwidth")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileMaxReadBandwidth);

    opts.AddLongOption(
            "performance-profile-max-write-bandwidth",
            "Volume performance profile max write bandwidth")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileMaxWriteBandwidth);

    opts.AddLongOption(
            "performance-profile-max-read-iops",
            "Volume performance profile max read iops")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileMaxReadIops);

    opts.AddLongOption(
            "performance-profile-max-write-iops",
            "Volume performance profile max write iops")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileMaxWriteIops);

    opts.AddLongOption(
            "performance-profile-burst-percentage",
            "Volume performance profile burst percentage")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileBurstPercentage);

    opts.AddLongOption(
            "performance-profile-max-postponed-weight",
            "Volume performance profile max postponed weight")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileMaxPostponedWeight);

    opts.AddLongOption(
            "performance-profile-boost-time",
            "Volume performance profile boost time")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileBoostTime);

    opts.AddLongOption(
            "performance-profile-boost-refill-time",
            "Volume performance profile boost refill time")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileBoostRefillTime);

    opts.AddLongOption(
            "performance-profile-boost-percentage",
            "Volume performance profile boost percentage")
        .RequiredArgument("NUM")
        .StoreResult(&PerformanceProfileBoostPercentage);

    opts.AddLongOption(
            "performance-profile-throttling-enabled",
            "Enables/disables throttling")
        .RequiredArgument("NUM")
        .NoArgument()
        .SetFlag(&PerformanceProfileThrottlingEnabled);
}

void ParseStorageMediaKind(
    const NLastGetopt::TOptsParseResultException& parseResult,
    const TString& storageMediaKindArg,
    NCloud::NProto::EStorageMediaKind& mediaKind)
{
    if (!parseResult.FindLongOptParseResult("storage-media-kind")) {
        return;
    }

    if (!ParseMediaKind(storageMediaKindArg, &mediaKind)) {
        ythrow yexception()
            << "Failed to parse storage media kind: " << storageMediaKindArg;
    }
}

}   // namespace NCloud::NBlockStore::NClient
