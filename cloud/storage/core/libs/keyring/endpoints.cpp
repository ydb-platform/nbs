#include "endpoints.h"

#include "keyring.h"

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
    TString RootKeyringDesc;
    TString EndpointsKeyringDesc;

public:
    TKeyringStorage(TString rootKeyringDesc, TString endpointsKeyringDesc)
        : RootKeyringDesc(std::move(rootKeyringDesc))
        , EndpointsKeyringDesc(std::move(endpointsKeyringDesc))
    {}

    TResultOrError<TVector<ui32>> GetEndpointIds() override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();

        TVector<ui32> endpointIds;
        for (auto keyring: keyrings) {
            endpointIds.push_back(keyring.GetId());
        }
        return endpointIds;
    }

    TResultOrError<TString> GetEndpoint(ui32 endpointId) override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();
        for (auto keyring: keyrings) {
            if (keyring.GetId() == endpointId) {
                return GetKeyringValue(keyring);
            }
        }

        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Failed to find endpoint with id " << endpointId);
    }

    TResultOrError<ui32> AddEndpoint(const TString& endpointSpec) override
    {
        Y_UNUSED(endpointSpec);
        // TODO:
        return MakeError(E_NOT_IMPLEMENTED, "Failed to add endpoint to storage");
    }

    NProto::TError RemoveEndpoint(ui32 endpointId) override
    {
        Y_UNUSED(endpointId);
        // TODO:
        return MakeError(E_NOT_IMPLEMENTED, "Failed to remove endpoint from storage");
    }

private:
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
    TFileStorage(TString dirPath)
        : DirPath(std::move(dirPath))
    {}

    TResultOrError<TVector<ui32>> GetEndpointIds() override
    {
        TGuard guard(Mutex);

        if (!DirPath.IsDirectory()) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to find directory " << DirPath.GetPath().Quote());
        }

        TVector<TFsPath> endpointFiles;
        DirPath.List(endpointFiles);

        TVector<ui32> endpointIds;
        for (const auto& endpointFile: endpointFiles) {
            auto keyringId = FromString<ui32>(endpointFile.GetName());
            endpointIds.push_back(keyringId);
        }
        return endpointIds;
    }

    TResultOrError<TString> GetEndpoint(ui32 endpointId) override
    {
        TGuard guard(Mutex);

        auto endpointFile = DirPath.Child(ToString(endpointId));
        if (!endpointFile.Exists()) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to find endpoint with id " << endpointId);
        }

        return ReadFile(endpointFile);
    }

    TResultOrError<ui32> AddEndpoint(const TString& endpointSpec) override
    {
        TGuard guard(Mutex);

        auto id = GetFreeId();
        TFsPath tmpFilePath(MakeTempName(nullptr, "endpoint"));
        TFileOutput(tmpFilePath).Write(endpointSpec);
        tmpFilePath.ForceRenameTo(DirPath.Child(ToString(id)));

        return id;
    }

    NProto::TError RemoveEndpoint(ui32 endpointId) override
    {
        TGuard guard(Mutex);

        auto filepath = DirPath.Child(ToString(endpointId));
        filepath.DeleteIfExists();
        return {};
    }

private:
    TResultOrError<TString> ReadFile(const TFsPath& filepath)
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

        return Strip(TFileInput(file).ReadAll());
    }

    ui32 GetFreeId()
    {
        auto idsOrError = GetEndpointIds();
        if (HasError(idsOrError)) {
            return 0;
        }

        auto ids = idsOrError.GetResult();
        std::sort(ids.begin(), ids.end());

        ui32 freeId = 1;
        for (size_t i = 0; i < ids.size(); ++i, ++freeId) {
            if (ids[i] != freeId) {
                break;
            }
        }
        return freeId;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointStoragePtr CreateKeyringEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc)
{
    return std::make_shared<TKeyringStorage>(
        std::move(rootKeyringDesc),
        std::move(endpointsKeyringDesc));
}

IEndpointStoragePtr CreateFileEndpointStorage(TString dirPath)
{
    return std::make_shared<TFileStorage>(std::move(dirPath));
}

}   // namespace NCloud
