#include "nvme_systemd.h"

#include <contrib/libs/systemd/src/systemd/sd-device.h>

#include <util/folder/path.h>

#include <cerrno>

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void Test(const TString& path)
{
    Cerr << "path = " << path << Endl;

    const TFsPath fspath{path};
    if (!fspath.Exists()) {
        Cerr << "COULD NOT READ THE PATH" << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return;
    }

    const TFsPath devPath = fspath.ReadLink();
    // devPath.RelativePath(fspath);

    Cerr << "devPath = " << devPath << Endl;
    // if (!devPath.Exists()) {
    //     Cerr << "COULD NOT READ THE LINK" << Endl;
    //     Cerr << "--------------------------------------------" << Endl;
    //     return;
    // }

    Cerr << "Inserting name = " << devPath.Basename() << Endl;

    sd_device* device = nullptr;
    int res = sd_device_new_from_subsystem_sysname(
        &device,
        "block",
        devPath.Basename().c_str());
    Cerr << "sd_device_new_from_subsystem_sysname res = " << res << Endl;
    // Cerr << "device = " << device << Endl;

    if (!device) {
        int err = errno;
        Cerr << "NO DEVICE! errno = " << err << " (" << ::strerror(err) << ")"
             << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return;
    }

    const char* value = nullptr;
    res = sd_device_get_property_value(device, "ID_SERIAL_SHORT", &value);
    int err = errno;
    Cerr << "sd_device_get_property_value res = " << res << "; errno = " << err
         << " (" << ::strerror(err) << ")" << Endl;
    if (value) {
        Cerr << "sd_device_get_property_value value = " << TString(value)
             << Endl;
    }
    Cerr << "--------------------------------------------" << Endl;
}

::sd_device* GetDevice(const TFsPath& path) {
    Cerr << "path = " << path << Endl;

    if (!path.Exists()) {
        Cerr << "COULD NOT READ THE PATH" << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return nullptr;
    }

    TString basename;
    if (path.IsSymlink()) {
        const TFsPath linkPath = path.ReadLink();
        Cerr << "linkPath = " << linkPath << Endl;
        basename = linkPath.Basename();
    } else {
        basename = path.Basename();
    }

    // devPath.RelativePath(fspath);

    // if (!devPath.Exists()) {
    //     Cerr << "COULD NOT READ THE LINK" << Endl;
    //     Cerr << "--------------------------------------------" << Endl;
    //     return;
    // }

    Cerr << "Inserting name = " << basename << Endl;

    ::sd_device* device = nullptr;
    int res = sd_device_new_from_subsystem_sysname(
        &device,
        "block",
        basename.c_str());
    Cerr << "sd_device_new_from_subsystem_sysname res = " << res << Endl;
    // Cerr << "device = " << device << Endl;

    if (res < 0 || !device) {
        int err = errno;
        Cerr << "NO DEVICE! errno = " << err << " (" << ::strerror(err) << ")"
             << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return nullptr;
    }

    return device;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSystemdNvmeManager::TSystemdNvmeManager(
    ITaskQueuePtr executor,
    TDuration timeout)
    : TNvmeManager(std::move(executor), timeout)
{}

TSystemdNvmeManager::~TSystemdNvmeManager() = default;

TResultOrError<TString> TSystemdNvmeManager::GetSerialNumber(
    const TString& path)
{

    ::sd_device* device = GetDevice(TFsPath(path));

    // FREE MEMORY
    const char* value = nullptr;
    int res = sd_device_get_property_value(device, "ID_SERIAL_SHORT", &value);
    int err = errno;
    Cerr << "sd_device_get_property_value res = " << res << "; errno = " << err
         << " (" << ::strerror(err) << ")" << Endl;
    if (value) {
        Cerr << "sd_device_get_property_value value = " << TString(value)
             << Endl;
        return TString(value);
    }
    return MakeError(E_FAIL, "something here");
}

TResultOrError<bool> TSystemdNvmeManager::IsSsd(const TString& path)
{
    ::sd_device* device = GetDevice(TFsPath(path));

    const char* value = nullptr;
    int res = sd_device_get_property_value(device, "ID_ATA_ROTATION_RATE_RPM", &value);

    // I DONT KNOW....

    if (res < 0) {
        return true;
    }

    return false;
}

}   // namespace NCloud::NBlockStore::NNvme
