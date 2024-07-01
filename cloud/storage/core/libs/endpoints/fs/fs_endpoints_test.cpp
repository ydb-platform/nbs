#include "fs_endpoints_test.h"

#include <cloud/storage/core/libs/endpoints/iface/endpoints_test.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileMutableEndpointStorage final
    : public IMutableEndpointStorage
{
private:
    const TFsPath DirPath;

public:
    TFileMutableEndpointStorage(TString dirPath)
        : DirPath(std::move(dirPath))
    {}

    NProto::TError Init() override
    {
        DirPath.MkDir();

        if (!DirPath.IsDirectory()) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to create directory " << DirPath.GetPath());
        }

        return {};
    }

    NProto::TError Remove() override
    {
        DirPath.ForceDelete();
        return {};
    }

    TResultOrError<TString> AddEndpoint(
        const TString& key,
        const TString& data) override
    {
        auto filepath = DirPath.Child(Base64EncodeUrl(key));
        TFile file(filepath, EOpenModeFlag::CreateAlways);
        TFileOutput(file).Write(data);
        return key;
    }

    NProto::TError RemoveEndpoint(const TString& key) override
    {
        DirPath.Child(Base64EncodeUrl(key)).DeleteIfExists();
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMutableEndpointStoragePtr CreateFileMutableEndpointStorage(TString dirPath)
{
    return std::make_shared<TFileMutableEndpointStorage>(
        std::move(dirPath));
}

}   // namespace NCloud
