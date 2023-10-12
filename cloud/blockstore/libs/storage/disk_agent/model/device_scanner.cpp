#include "device_scanner.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/blockstore/libs/storage/core/config.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <sys/stat.h>

#include <filesystem>
#include <regex>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetBlockSize(const std::string& path)
{
    struct stat s {};

    if (int r = ::stat(path.c_str(), &s)) {
        const int ec = errno;
        ythrow TServiceError {MAKE_SYSTEM_ERROR(ec)}
            << "can't get information about a file " << path << ": "
            << ::strerror(ec);
    }

    return s.st_blksize;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError FindDevices(
    const NProto::TStorageDiscoveryConfig& config,
    TDeviceCallback cb)
{
    namespace NFs = std::filesystem;

    try {
        for (const auto& p: config.GetPathConfigs()) {
            const std::string pattern = p.GetPathRegExp();
            const std::regex re {pattern, std::regex_constants::ECMAScript};
            const auto parentPath = NFs::path {pattern}.parent_path();

            for (const auto& entry: NFs::directory_iterator {parentPath}) {
                const std::string path = entry.path().string();

                std::smatch match;
                if (!std::regex_match(path, match, re)) {
                    continue;
                }

                if (!entry.is_regular_file() && !entry.is_block_file()) {
                    return MakeError(E_ARGUMENT, TStringBuilder()
                        << path << " is not a file or a block device ");
                }

                if (match.size() < 2) {
                    return MakeError(E_ARGUMENT, TStringBuilder()
                        << "path " << path << " accepted but regexp "
                        << p.GetPathRegExp() << " doesn't contain any group of digits");
                }

                const ui32 deviceNumber = std::stoul(match[1].str());
                if (!deviceNumber) {
                    return MakeError(E_ARGUMENT, TStringBuilder()
                        << path << ": the device number can't be zero");
                }

                const ui64 size = entry.file_size();
                auto* pool = FindIfPtr(p.GetPoolConfigs(), [&] (const auto& pool) {
                    return pool.GetMinSize() <= size && size <= pool.GetMaxSize();
                });

                if (!pool) {
                    return MakeError(E_NOT_FOUND, TStringBuilder()
                        << "unable to find the appropriate pool for " << path);
                }

                cb(
                    path.c_str(),
                    *pool,
                    deviceNumber,
                    p.GetMaxDeviceCount(),
                    GetBlockSize(path),
                    size);
            }
        }
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
