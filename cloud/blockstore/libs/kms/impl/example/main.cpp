#include <cloud/blockstore/libs/kms/iface/kms_client.h>
#include <cloud/blockstore/libs/kms/impl/kms_client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/scope.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud;

    NLastGetopt::TOpts opts;
    opts.AddLongOption("address", "kms server address").Required();
    opts.AddLongOption("key-id", "kms key id").Required();
    opts.AddLongOption("ciphertext", "base64 of encrypted dek").Required();
    opts.AddLongOption("token", "IAM token").Required();
    opts.AddLongOption("insecure", "disable TLS").NoArgument();
    opts.AddLongOption("target-name", "ssl target name override")
        .DefaultValue("");
    opts.AddHelpOption();

    TString address;
    bool insecure;
    TString keyId;
    TString ciphertext;
    TString token;
    TString targetName;

    try {
        NLastGetopt::TOptsParseResult result(&opts, argc, argv);
        address = result.Get("address");
        insecure = result.Has("insecure");
        keyId = result.Get("key-id");
        ciphertext = Base64Decode(result.Get("ciphertext"));
        token = result.Get("token");
        targetName = result.Get("target-name");
    } catch (const yexception& ex) {
        Cerr << "Error. " << ex.what() << Endl;
        opts.PrintUsage("kms-example");
        return 1;
    }

    NBlockStore::NProto::TGrpcClientConfig config;
    config.SetAddress(address);
    config.SetRequestTimeout(5000);
    config.SetInsecure(insecure);
    config.SetSslTargetNameOverride(targetName);

    auto logging = CreateLoggingService("console");
    auto client = NBlockStore::CreateKmsClient(logging, config);

    client->Start();
    Y_DEFER
    {
        client->Stop();
    }

    auto future = client->Decrypt(keyId, ciphertext, token);
    auto response = future.GetValueSync();
    if (HasError(response)) {
        Cerr << "Error. " << FormatError(response.GetError()) << Endl;
        Sleep(TDuration::MilliSeconds(250));
        return 1;
    }

    Cerr << "Success. " << Base64Encode(response.GetResult()) << Endl;
    return 0;
}
