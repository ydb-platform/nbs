#include "options.h"

#include <util/generic/serialized_enum.h>

namespace NCloud::NBlockStore::NServer {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////
TOptionsYdb::TOptionsYdb()
{
    Opts.AddLongOption("storage-file")
        .RequiredArgument("FILE")
        .StoreResult(&StorageConfig);

    Opts.AddLongOption("ydbstats-file")
        .RequiredArgument("FILE")
        .StoreResult(&StatsUploadConfig);

    Opts.AddLongOption("features-file")
        .RequiredArgument("FILE")
        .StoreResult(&FeaturesConfig);

    Opts.AddLongOption("logbroker-file")
        .RequiredArgument("PATH")
        .StoreResult(&LogbrokerConfig);

    Opts.AddLongOption("notify-file")
        .RequiredArgument("PATH")
        .StoreResult(&NotifyConfig);

    Opts.AddLongOption("iam-file")
        .RequiredArgument("PATH")
        .StoreResult(&IamConfig);

    Opts.AddLongOption("kms-file")
        .RequiredArgument("PATH")
        .StoreResult(&KmsConfig);

    Opts.AddLongOption("root-kms-file")
        .RequiredArgument("PATH")
        .StoreResult(&RootKmsConfig);

    Opts.AddLongOption("compute-file")
        .RequiredArgument("PATH")
        .StoreResult(&ComputeConfig);

    Opts.AddLongOption("trace-file")
        .RequiredArgument("PATH")
        .StoreResult(&TraceServiceConfig);
}

void TOptionsYdb::Parse(int argc, char** argv)
{
    auto res = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);
    if (res->FindLongOptParseResult("verbose") != nullptr && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    if (ServiceKind == EServiceKind::Ydb) {
        Y_ENSURE(res->FindLongOptParseResult("ic-port"),
            "'--ic-port' option is required for kikimr service");

        Y_ENSURE(res->FindLongOptParseResult("domain"),
            "'--domain' option is required for kikimr service");

        Y_ENSURE(SysConfig && DomainsConfig || LoadCmsConfigs,
            "sys-file and domains-file options are required if load-configs-from-cms is not set");
    }
}

}   // namespace NCloud::NBlockStore::NServer
