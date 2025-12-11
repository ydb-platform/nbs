#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/getopt/small/last_getopt.h>

namespace {

using namespace NCloud;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString CloudId;
    TString FolderId;
    TString EntityId;
    TString FeaturesPath;

    void Parse(int argc, char** argv)
    {
        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("cloud", "cloud id")
            .RequiredArgument()
            .StoreResult(&CloudId);

        opts.AddLongOption("folder", "folder id")
            .RequiredArgument()
            .StoreResult(&FolderId);

        opts.AddLongOption("entity", "disk or fs id")
            .RequiredArgument()
            .StoreResult(&EntityId);

        opts.AddLongOption("features", "path to features config")
            .RequiredArgument()
            .Required()
            .StoreResult(&FeaturesPath);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions opts;
    opts.Parse(argc, argv);

    NCloud::NProto::TFeaturesConfig proto;
    ParseProtoTextFromFileRobust(opts.FeaturesPath, proto);

    NCloud::NFeatures::TFeaturesConfig config(std::move(proto));

    for (const auto& f: config.CollectAllFeatures()) {
        const bool enabled = config.IsFeatureEnabled(
            opts.CloudId,
            opts.FolderId,
            opts.EntityId,
            f);
        const auto v = config.GetFeatureValue(
            opts.CloudId,
            opts.FolderId,
            opts.EntityId,
            f);

        Cout << "Feature " << f << " " << (enabled ? "enabled" : "disabled");
        if (v) {
            Cout << " value=" << v;
        }
        Cout << Endl;
    }

    return 0;
}
