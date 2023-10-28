#include <cloud/blockstore/libs/storage/core/features_config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/getopt/small/last_getopt.h>

namespace {

using namespace NCloud;
using namespace NBlockStore;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString CloudId;
    TString FolderId;
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

    NProto::TFeaturesConfig proto;
    ParseProtoTextFromFileRobust(opts.FeaturesPath, proto);

    NBlockStore::NStorage::TFeaturesConfig config(std::move(proto));

    for (const auto& f: config.CollectAllFeatures()) {
        bool enabled = config.IsFeatureEnabled(opts.CloudId, opts.FolderId, f);
        auto v = config.GetFeatureValue(opts.CloudId, opts.FolderId, f);

        Cout << "Feature " << f
            << " " << (enabled ? "enabled" : "disabled");
        if (v) {
            Cout << " value=" << v;
        }
        Cout << Endl;
    }

    return 0;
}
