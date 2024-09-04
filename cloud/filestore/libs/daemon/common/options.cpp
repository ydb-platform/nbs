#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/serialized_enum.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptionsCommon::TOptionsCommon()
{
    Opts.AddVersionOption();

    Opts.AddLongOption("app-config")
        .RequiredArgument("FILE")
        .StoreResult(&AppConfig);

    Opts.AddLongOption("storage-file")
        .RequiredArgument("FILE")
        .StoreResult(&StorageConfig);

    Opts.AddLongOption("features-file")
        .RequiredArgument("FILE")
        .StoreResult(&FeaturesConfig);

    Opts.AddLongOption("node-registration-attempts")
        .RequiredArgument("NUM")
        .StoreResult(&NodeRegistrationMaxAttempts);

    Opts.AddLongOption("node-registration-timeout")
        .RequiredArgument("DURATION")
        .StoreResult(&NodeRegistrationTimeout);

    Opts.AddLongOption("node-registration-error-timeout")
        .RequiredArgument("DURATION")
        .StoreResult(&NodeRegistrationErrorTimeout);

    Opts.AddLongOption("service")
        .RequiredArgument("{" + GetEnumAllNames<EServiceKind>() + "}")
        .Handler1T<TString>([this] (const auto& s) {
            Service = FromString<EServiceKind>(s);
        });

    Opts.AddLongOption("disable-local-service")
        .StoreTrue(&DisableLocalService);
}

void TOptionsCommon::Parse(int argc, char** argv)
{
    auto res = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);
    if (res->FindLongOptParseResult("verbose") != nullptr && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    if (Service == EServiceKind::Kikimr) {
        Y_ENSURE(res->FindLongOptParseResult("ic-port"),
        "'--ic-port' option is required for kikimr service");

        Y_ENSURE(res->FindLongOptParseResult("domain"),
        "'--domain' option is required for kikimr service");

        Y_ENSURE(SysConfig && DomainsConfig,
        "sys-file and domains-file options are required if load-configs-from-cms is not set");
    }
}

} // NCloud::NFileStore::NDaemon
