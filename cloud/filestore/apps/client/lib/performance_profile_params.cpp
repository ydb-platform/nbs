#include "performance_profile_params.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TPerformanceProfileParams::TPerformanceProfileParams(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption(
            "performance-profile-throttling-enabled",
            "Enables/disables throttling")
        .RequiredArgument("NUM")
        .NoArgument()
        .SetFlag(&ThrottlingEnabled);

    opts.AddLongOption(
            "performance-profile-max-read-bandwidth",
            "File system performance profile max read bandwidth (in bytes)")
        .RequiredArgument("NUM")
        .StoreResult(&MaxReadBandwidth);

    opts.AddLongOption(
            "performance-profile-max-read-iops",
            "File system performance profile max read iops")
        .RequiredArgument("NUM")
        .StoreResult(&MaxReadIops);

    opts.AddLongOption(
            "performance-profile-max-write-bandwidth",
            "File system performance profile max write bandwidth (in bytes)")
        .RequiredArgument("NUM")
        .StoreResult(&MaxWriteBandwidth);

    opts.AddLongOption(
            "performance-profile-max-write-iops",
            "File system performance profile max write iops")
        .RequiredArgument("NUM")
        .StoreResult(&MaxWriteIops);

    opts.AddLongOption(
            "performance-profile-boost-time",
            "File system performance profile boost time (in ms)")
        .RequiredArgument("NUM")
        .StoreResult(&BoostTime);

    opts.AddLongOption(
            "performance-profile-boost-refill-time",
            "File system performance profile boost refill time (in ms)")
        .RequiredArgument("NUM")
        .StoreResult(&BoostRefillTime);

    opts.AddLongOption(
            "performance-profile-boost-percentage",
            "File system performance profile boost percentage")
        .RequiredArgument("NUM")
        .StoreResult(&BoostPercentage);

    opts.AddLongOption(
            "performance-profile-burst-percentage",
            "File system performance profile burst percentage")
        .RequiredArgument("NUM")
        .StoreResult(&BurstPercentage);

    opts.AddLongOption(
            "performance-profile-default-postponed-request-weight",
            "File system performance profile default postponed request weight")
        .RequiredArgument("NUM")
        .StoreResult(&DefaultPostponedRequestWeight);

    opts.AddLongOption(
            "performance-profile-max-postponed-weight",
            "File system performance profile max postponed weight")
        .RequiredArgument("NUM")
        .StoreResult(&MaxPostponedWeight);

    opts.AddLongOption(
            "performance-profile-max-write-cost-multiplier",
            "File system performance profile max write cost multiplier")
        .RequiredArgument("NUM")
        .StoreResult(&MaxWriteCostMultiplier);

    opts.AddLongOption(
            "performance-profile-max-postponed-time",
            "File system performance profile max postponed time")
        .RequiredArgument("NUM")
        .StoreResult(&MaxPostponedTime);

    opts.AddLongOption(
            "performance-profile-max-postponed-count",
            "File system performance profile max postponed count")
        .RequiredArgument("NUM")
        .StoreResult(&MaxPostponedCount);
}

}   // namespace NCloud::NFileStore::NClient
