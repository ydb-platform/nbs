#include "endpoints_test.h"

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKeyringMutableEndpointStorage final
    : public IMutableEndpointStorage
{
private:
    const TString RootKeyringDesc;
    const TString SubKeyringDesc;

    TKeyring EndpointsKeyring;

public:
    TKeyringMutableEndpointStorage(
            TString rootKeyringDesc,
            TString endpointsKeyringDesc)
        : RootKeyringDesc(std::move(rootKeyringDesc))
        , SubKeyringDesc(std::move(endpointsKeyringDesc))
    {}

    ~TKeyringMutableEndpointStorage()
    {
        Remove();
    }

    NProto::TError Init() override;
    NProto::TError Remove() override;

    TResultOrError<ui32> AddEndpoint(
        const TString& key,
        const TString& data) override;

    NProto::TError RemoveEndpoint(
        const TString& key) override;
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError TKeyringMutableEndpointStorage::Init()
{
    auto perm = (
        TKeyring::PosAllPerm |
        TKeyring::UsrAllPerm |
        TKeyring::GrpAllPerm |
        TKeyring::OthAllPerm);

    auto rootKeyring = TKeyring::GetProcKey(RootKeyringDesc);

    if (!rootKeyring) {
        auto process = TKeyring::GetRoot(TKeyring::Process);
        if (!process) {
            return MakeError(E_FAIL, "Failed to get process keyring");
        }

        auto user = TKeyring::GetRoot(TKeyring::User);
        if (!user) {
            return MakeError(E_FAIL, "Failed to get user keyring");
        }

        rootKeyring = process.AddKeyring(RootKeyringDesc);
        if (!rootKeyring) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to add keyring " << RootKeyringDesc.Quote());
        }

        if (!rootKeyring.SetPerm(perm)) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to set perm for keyring " << RootKeyringDesc.Quote());
        }

        if (!user.LinkKeyring(rootKeyring)) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to link keyring " << RootKeyringDesc.Quote()
                << " to user keyring");
        }
    }

    auto subKeyring = rootKeyring.SearchKeyring(SubKeyringDesc);

    if (!subKeyring) {
        subKeyring = rootKeyring.AddKeyring(SubKeyringDesc);
        if (!subKeyring) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to add keyring " << SubKeyringDesc.Quote());
        }

        if (!subKeyring.SetPerm(perm)) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to set perm for keyring " << SubKeyringDesc.Quote());
        }
    }

    EndpointsKeyring = subKeyring;
    return {};
}

NProto::TError TKeyringMutableEndpointStorage::Remove()
{
    auto process = TKeyring::GetRoot(TKeyring::Process);
    if (!process) {
        return MakeError(E_FAIL, "failed to get process keyring");
    }

    auto rootKeyring = process.SearchKeyring(RootKeyringDesc);
    if (!rootKeyring) {
        return MakeError(E_FAIL, TStringBuilder()
            << "failed to find root keyring " << RootKeyringDesc.Quote());
    }

    if (!process.UnlinkKeyring(rootKeyring)) {
        return MakeError(E_FAIL, TStringBuilder()
            << "failed to unlink root keyring "
            << RootKeyringDesc.Quote() << " from process keyring");
    }

    auto user = TKeyring::GetRoot(TKeyring::User);
    if (!user) {
        return MakeError(E_FAIL, "failed to get user keyring");
    }

    if (!user.UnlinkKeyring(rootKeyring)) {
        return MakeError(E_FAIL, TStringBuilder()
            << "failed to unlink root keyring "
            << RootKeyringDesc.Quote() << " from user keyring");
    }

    // wait while /proc/keys is updating
    while (TKeyring::GetProcKey(RootKeyringDesc).GetId() == rootKeyring.GetId()) {
        Sleep(TDuration::MilliSeconds(100));
    }

    return {};
}

TResultOrError<ui32> TKeyringMutableEndpointStorage::AddEndpoint(
    const TString& key,
    const TString& data)
{
    auto perm = (
        TKeyring::PosAllPerm |
        TKeyring::UsrAllPerm |
        TKeyring::GrpAllPerm |
        TKeyring::OthAllPerm);

    auto userKey = EndpointsKeyring.AddUserKey(key, data);
    if (!userKey) {
        return MakeError(E_FAIL, TStringBuilder()
            << "Failed to set perm for keyring " << key.Quote());
    }

    userKey.SetPerm(perm);
    return userKey.GetId();
}

NProto::TError TKeyringMutableEndpointStorage::RemoveEndpoint(
    const TString& key)
{
    bool found = false;

    auto userKeys = EndpointsKeyring.GetUserKeys();
    for (auto userKey: userKeys) {
        if (userKey.GetDesc() != key) {
            continue;
        }
        found = true;

        if (!EndpointsKeyring.UnlinkKeyring(userKey)) {
            return MakeError(E_FAIL, TStringBuilder()
                << "Failed to unlink keyring " << key.Quote());
        }
    }

    if (!found) {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Failed to find keyring " << key.Quote() << " to unlink");
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TFileMutableEndpointStorage final
    : public IMutableEndpointStorage
{
private:
    const TFsPath DirPath;
    ui32 LastKeyringId = 0;

    THashMap<TString, ui32> KeyMap;

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

    TResultOrError<ui32> AddEndpoint(
        const TString& key,
        const TString& data) override
    {
        ++LastKeyringId;

        KeyMap.emplace(key, LastKeyringId);

        auto filepath = DirPath.Child(ToString(LastKeyringId));
        TFile file(filepath, EOpenModeFlag::CreateAlways);
        TFileOutput(file).Write(data);
        return LastKeyringId;
    }

    NProto::TError RemoveEndpoint(const TString& key) override
    {
        auto it = KeyMap.find(key);
        if (it == KeyMap.end()) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Failed to find keyring " << key.Quote() << " to unlink");
        }

        auto keyringId = it->second;
        DirPath.Child(ToString(keyringId)).DeleteIfExists();
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMutableEndpointStoragePtr CreateKeyringMutableEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc)
{
    return std::make_shared<TKeyringMutableEndpointStorage>(
        std::move(rootKeyringDesc),
        std::move(endpointsKeyringDesc));
}

IMutableEndpointStoragePtr CreateFileMutableEndpointStorage(TString dirPath)
{
    return std::make_shared<TFileMutableEndpointStorage>(
        std::move(dirPath));
}

}   // namespace NCloud
