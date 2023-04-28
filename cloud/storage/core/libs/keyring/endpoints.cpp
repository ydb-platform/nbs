#include "endpoints.h"

#include "keyring.h"

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/file.h>

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

    TResultOrError<TString> GetEndpoint(ui32 keyringId) override
    {
        auto keyringsOrError = GetEndpointKeyrings();
        if (HasError(keyringsOrError)) {
            return keyringsOrError.GetError();
        }

        auto keyrings = keyringsOrError.ExtractResult();
        for (auto keyring: keyrings) {
            if (keyring.GetId() == keyringId) {
                return GetKeyringValue(keyring);
            }
        }

        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Failed to find endpoint with keyringId " << keyringId);
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

public:
    TFileStorage(TString dirPath)
        : DirPath(std::move(dirPath))
    {}

    TResultOrError<TVector<ui32>> GetEndpointIds() override
    {
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

    TResultOrError<TString> GetEndpoint(ui32 keyringId) override
    {
        auto endpointFile = DirPath.Child(ToString(keyringId));
        if (!endpointFile.Exists()) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to find endpoint with keyringId " << keyringId);
        }

        return ReadFile(endpointFile);
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
