#include "fs_endpoints.h"

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/tempfile.h>

namespace NCloud {

namespace {

class TFileStorage final: public IEndpointStorage
{
private:
    const TFsPath DirPath;
    TMutex Mutex;

public:
    explicit TFileStorage(const TString& dirPath)
        : DirPath(dirPath)
    {}

    TResultOrError<TVector<TString>> GetEndpointIds() override
    {
        TGuard guard(Mutex);

        if (!DirPath.IsDirectory()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "Failed to find directory "
                                 << DirPath.GetPath().Quote());
        }

        TVector<TFsPath> endpointFiles;
        try {
            DirPath.List(endpointFiles);
        } catch (...) {
            return MakeError(E_IO, CurrentExceptionMessage());
        }

        TVector<TString> endpointIds;
        for (const auto& endpointFile: endpointFiles) {
            auto [endpointId, error] = SafeBase64Decode(endpointFile.GetName());
            if (HasError(error)) {
                // TODO: ReportCritEvent()
                continue;
            }
            endpointIds.push_back(endpointId);
        }
        return endpointIds;
    }

    TResultOrError<TString> GetEndpoint(const TString& endpointId) override
    {
        TGuard guard(Mutex);

        auto endpointFile = DirPath.Child(Base64EncodeUrl(endpointId));
        if (!endpointFile.Exists()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Failed to find endpoint with id " << endpointId);
        }

        return ReadFile(endpointFile);
    }

    NProto::TError AddEndpoint(
        const TString& endpointId,
        const TString& endpointSpec) override
    {
        TGuard guard(Mutex);

        try {
            TFsPath tmpFilePath(MakeTempName(nullptr, "endpoint"));
            TFileOutput(tmpFilePath).Write(endpointSpec);
            tmpFilePath.ForceRenameTo(
                DirPath.Child(Base64EncodeUrl(endpointId)));
            return {};
        } catch (...) {
            return MakeError(E_IO, CurrentExceptionMessage());
        }
    }

    NProto::TError RemoveEndpoint(const TString& endpointId) override
    {
        TGuard guard(Mutex);

        try {
            auto filepath = DirPath.Child(Base64EncodeUrl(endpointId));
            filepath.DeleteIfExists();
            return {};
        } catch (...) {
            return MakeError(E_IO, CurrentExceptionMessage());
        }
    }

private:
    static TResultOrError<TString> ReadFile(const TFsPath& filepath)
    {
        TFile file;
        try {
            file = TFile(
                filepath,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        } catch (...) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Failed to open file " << filepath.GetPath().Quote());
        }

        if (!file.IsOpen()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Failed to open file " << filepath.GetPath().Quote());
        }

        try {
            return TFileInput(file).ReadAll();
        } catch (...) {
            return MakeError(E_IO, CurrentExceptionMessage());
        }
    }

    static TResultOrError<TString> SafeBase64Decode(const TString& s)
    {
        try {
            return Base64Decode(s);
        } catch (...) {
            return MakeError(E_ARGUMENT, CurrentExceptionMessage());
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointStoragePtr CreateFileEndpointStorage(TString dirPath)
{
    return std::make_shared<TFileStorage>(std::move(dirPath));
}

}   // namespace NCloud
