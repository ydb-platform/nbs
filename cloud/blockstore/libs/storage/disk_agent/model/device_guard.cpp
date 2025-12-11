#include "device_guard.h"

#include <util/folder/path.h>

#include <ranges>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TFsPath GetRealPath(TFsPath path)
{
    while (path.IsSymlink()) {
        path = path.ReadLink();
    }
    return path;
}

TVector<TString> GetPrefixPaths(const TFsPath& path)
{
    if (!path.Exists()) {
        return {};
    }

    const auto realPath = GetRealPath(path);
    TVector<TFsPath> files;
    realPath.Parent().List(files);
    TVector<TString> result;
    for (const auto& file: files) {
        if (path.GetPath().StartsWith(file.GetPath())) {
            result.push_back(file);
        }
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TDeviceGuard::Lock(const TString& path)
{
    auto prefixPaths = GetPrefixPaths(path);
    if (prefixPaths.empty()) {
        return false;
    }

    TVector<TFileHandle> lockedFiles;
    lockedFiles.reserve(prefixPaths.size());

    for (const auto& pp: prefixPaths) {
        if (auto it = Storage.find(pp); it != Storage.end()) {
            TFileHandle fileHandle(it->second.Duplicate());
            if (!fileHandle.IsOpen()) {
                break;
            }
            lockedFiles.emplace_back(std::move(fileHandle));
            continue;
        }

        TFileHandle fileHandle(
            pp,
            EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly |
                EOpenModeFlag::WrOnly);
        if (!fileHandle.IsOpen() || fileHandle.Flock(LOCK_EX | LOCK_NB)) {
            break;
        }
        lockedFiles.emplace_back(std::move(fileHandle));
    }

    if (lockedFiles.size() != prefixPaths.size()) {
        return false;
    }

    for (size_t i = 0; i < lockedFiles.size(); ++i) {
        Storage.emplace(std::move(prefixPaths[i]), std::move(lockedFiles[i]));
    }
    return true;
}

bool TDeviceGuard::Unlock(const TString& path)
{
    auto prefixPaths = GetPrefixPaths(path);
    if (prefixPaths.empty()) {
        return false;
    }

    TVector<TFileHandle> unlockedFiles;
    unlockedFiles.reserve(prefixPaths.size());

    for (const auto& pp: prefixPaths) {
        if (auto it = Storage.find(pp); it != Storage.end()) {
            unlockedFiles.emplace_back(std::move(it->second));
            Storage.erase(it);
        }
    }

    if (unlockedFiles.size() == prefixPaths.size()) {
        return true;
    }

    for (size_t i = 0; i < unlockedFiles.size(); ++i) {
        Storage.emplace(std::move(prefixPaths[i]), std::move(unlockedFiles[i]));
    }
    return false;
}

}   // namespace NCloud::NBlockStore::NStorage
