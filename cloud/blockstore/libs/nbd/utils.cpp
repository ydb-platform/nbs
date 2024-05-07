#include "utils.h"

#include <util/generic/set.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

#include <filesystem>

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString CleanBackingFilePath(TString path)
{
    // If the block device was deleted, the path will contain a "(deleted)" suffix
    static const TString suffix = "(deleted)";
    path = Strip(path);
    if (path.EndsWith(suffix)) {
        path = path.substr(0, path.length() - suffix.length());
    }
    return Strip(path);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static bool IsTcpAddress(int family)
{
    return family == AF_INET || family == AF_INET6;
}

static bool IsUnixAddress(int family)
{
    return family == AF_UNIX;
}

bool IsTcpAddress(const NAddr::IRemoteAddr& addr)
{
    return IsTcpAddress(addr.Addr()->sa_family);
}

bool IsTcpAddress(const TNetworkAddress& addr)
{
    return IsTcpAddress(addr.Begin()->ai_family);
}

bool IsUnixAddress(const TNetworkAddress& addr)
{
    return IsUnixAddress(addr.Begin()->ai_family);
}

TString PrintHostAndPort(const TNetworkAddress& addr)
{
    TStringStream out;
    for (auto it = addr.Begin(), end = addr.End(); it != end; ++it) {
        out << NAddr::PrintHostAndPort(NAddr::TAddrInfo(&*it));
        out << " ";
    }
    return out.Str();
}

TSet<TString> FindMountedFiles(
    const TString& device,
    const TString& mountInfoFile)
{
    static constexpr int deviceNameColumn = 3;
    static constexpr int mountedFileColumn = 4;
    static constexpr int correctColumnCount = 11;

    if (!device.StartsWith("/dev/")) {
        return {};
    }

    auto deviceName = device.substr(4);
    TSet<TString> mountedFiles;
    mountedFiles.insert(device);

    if (!NFs::Exists(mountInfoFile)) {
        return mountedFiles;
    }

    TIFStream is(mountInfoFile);
    TString line;
    while (is.ReadLine(line)) {
        TVector<TString> items;
        auto columnCount = Split(line, " ", items);
        if (columnCount != correctColumnCount) {
            continue;
        }

        if (items[deviceNameColumn] == deviceName) {
            mountedFiles.insert(items[mountedFileColumn]);
        }
    }

    return mountedFiles;
}

TVector<TString> FindLoopbackDevices(
    const TSet<TString>& mountedFiles,
    const TString& sysBlockDir)
{
    TVector<TString> loopbackDevices;

    auto sysBlockItems = std::filesystem::directory_iterator{
        sysBlockDir.c_str()};

    for (const auto& entry: sysBlockItems) {
        if (!entry.is_directory()) {
            continue;
        }

        TString loopPath = entry.path().string();
        if (!loopPath.StartsWith(sysBlockDir + "loop")) {
            continue;
        }

        auto backingFile = loopPath + "/loop/backing_file";
        if (!NFs::Exists(backingFile)) {
            continue;
        }

        TFileInput in(backingFile);
        auto data = in.ReadAll();
        auto backingFilePath = CleanBackingFilePath(data);
        if (!mountedFiles.contains(backingFilePath)) {
            continue;
        }

        auto loopDevice = "/dev/" + TFsPath(loopPath).GetName();
        loopbackDevices.push_back(loopDevice);
    }

    return loopbackDevices;
}

int RemoveLoopbackDevice(const TString& loopDevice)
{
    TString cmd = "losetup -d " + loopDevice;
    return std::system(cmd.c_str());
}

TString FindFreeNbdDevice(const TString& sysBlockDir)
{
    auto sysBlockItems = std::filesystem::directory_iterator{
        sysBlockDir.c_str()};

    for (const auto& entry: sysBlockItems) {
        if (!entry.is_directory()) {
            continue;
        }

        auto nbd = entry.path().filename().string();
        if (nbd.rfind("nbd", 0)) {
            continue;
        }

        if (std::filesystem::exists(entry.path() / "pid")) {
            continue;
        }

        return "/dev/" + nbd;
    }

    return "";
}

}   // namespace NCloud::NBlockStore::NBD
