#include "endpoints.h"

#include "keyring.h"

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

////////////////////////////////////////////////////////////////////////////////

class TKeyringStorage final
    : public IEndpointStorage
{
private:
    const TString RootKeyringDesc;
    const TString EndpointsKeyringDesc;
    const bool NotImplementedErrorIsFatal;

public:
    TKeyringStorage(
            TString rootKeyringDesc,
            TString endpointsKeyringDesc,
            bool notImplementedErrorIsFatal)
        : RootKeyringDesc(std::move(rootKeyringDesc))
        , EndpointsKeyringDesc(std::move(endpointsKeyringDesc))
        , NotImplementedErrorIsFatal(notImplementedErrorIsFatal)
    {}

    TResultOrError<TVector<TString>> GetEndpointIds() override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();

        TVector<TString> endpointIds;
        for (auto keyring: keyrings) {
            endpointIds.push_back(ToString(keyring.GetId()));
        }
        return endpointIds;
    }

    TResultOrError<TString> GetEndpoint(const TString& endpointId) override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();
        for (auto keyring: keyrings) {
            if (ToString(keyring.GetId()) == endpointId) {
                return GetKeyringValue(keyring);
            }
        }

        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Failed to find endpoint with id " << endpointId);
    }

    NProto::TError AddEndpoint(
        const TString& endpointId,
        const TString& endpointSpec) override
    {
        Y_UNUSED(endpointId);
        Y_UNUSED(endpointSpec);
        // TODO:
        return NotImplemented("Failed to add endpoint to storage");
    }

    NProto::TError RemoveEndpoint(const TString& endpointId) override
    {
        Y_UNUSED(endpointId);
        // TODO:
        return NotImplemented("Failed to remove endpoint from storage");
    }

private:
    NProto::TError NotImplemented(TString message) const
    {
        ui32 flags = 0;
        if (!NotImplementedErrorIsFatal) {
            SetProtoFlag(flags, NProto::EF_SILENT);
        }
        return MakeError(E_NOT_IMPLEMENTED, std::move(message), flags);
    }

    TResultOrError<TVector<TKeyring>> GetEndpointKeyrings()
    {
        if (RootKeyringDesc.empty() || EndpointsKeyringDesc.empty()) {
            return TVector<TKeyring>();
        }

        return SafeExecute<TResultOrError<TVector<TKeyring>>>([&] {
            auto rootKeyring = TKeyring::GetProcKey(RootKeyringDesc);
            if (!rootKeyring) {
                ythrow TServiceError(E_INVALID_STATE)
                    << "Failed to find root keyring "
                    << RootKeyringDesc.Quote();
            }

            auto endpointsKeyring = rootKeyring.SearchKeyring(
                EndpointsKeyringDesc);

            if (!endpointsKeyring) {
                ythrow TServiceError(E_INVALID_STATE)
                    << "Failed to find endpoints keyring "
                    << EndpointsKeyringDesc.Quote();
            }

            return endpointsKeyring.GetUserKeys();
        });
    }

    TResultOrError<TString> GetKeyringValue(TKeyring keyring)
    {
        return SafeExecute<TResultOrError<TString>>([&] {
            return keyring.GetValue();
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileStorage final
    : public IEndpointStorage
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
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to find directory " << DirPath.GetPath().Quote());
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
            return MakeError(E_INVALID_STATE, TStringBuilder()
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
            tmpFilePath.ForceRenameTo(DirPath.Child(Base64EncodeUrl(endpointId)));
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
            file = TFile(filepath,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        } catch (...) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to open file " << filepath.GetPath().Quote());
        }

        if (!file.IsOpen()) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
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

IEndpointStoragePtr CreateKeyringEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc,
    bool notImplementedErrorIsFatal)
{
    return std::make_shared<TKeyringStorage>(
        std::move(rootKeyringDesc),
        std::move(endpointsKeyringDesc),
        notImplementedErrorIsFatal);
}

IEndpointStoragePtr CreateFileEndpointStorage(TString dirPath)
{
    return std::make_shared<TFileStorage>(std::move(dirPath));
}

}   // namespace NCloud
