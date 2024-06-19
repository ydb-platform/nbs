#include "nvme_systemd.h"

#include <contrib/libs/systemd/src/systemd/sd-device.h>

#include <util/folder/path.h>
#include <util/string/builder.h>

#include <cerrno>

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

struct TSdDeviceDeleter
{
    void operator()(::sd_device* device)
    {
        if (device) {
            sd_device_unref(device);
        }
    }
};
using SdDevicePtr = std::unique_ptr<::sd_device, TSdDeviceDeleter>;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<SdDevicePtr> GetDevice(const TFsPath& path)
{
    Cerr << "path = " << path << Endl;

    if (!path.Exists()) {
        return MakeError(E_FAIL, "COULD NOT READ THE PATH");
    }

    TString basename;
    if (path.IsSymlink()) {
        const TFsPath linkPath = path.ReadLink();
        Cerr << "linkPath = " << linkPath << Endl;
        basename = linkPath.Basename();
    } else {
        basename = path.Basename();
    }

    Cerr << "Inserting name = " << basename << Endl;

    SdDevicePtr device;
    int res = 0;
    {
        ::sd_device* devicePtr = nullptr;
        res = sd_device_new_from_subsystem_sysname(
            &devicePtr,
            "block",
            basename.c_str());
        device.reset(devicePtr);
    }

    Cerr << "sd_device_new_from_subsystem_sysname res = " << res << Endl;

    if (res < 0 || !device) {
        int err = errno;
        return MakeError(
            MAKE_SYSTEM_ERROR(err),
            TStringBuilder() << "NO DEVICE! " << ::strerror(err));
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
    auto result = GetDevice(TFsPath(path));
    if (result.HasError()) {
        return result.GetError();
    }

    const char* value = nullptr;
    int res = sd_device_get_property_value(
        result.ExtractResult().get(),
        "ID_SERIAL_SHORT",
        &value);
    if (res < 0 || !value) {
        int err = errno;
        return MakeError(
            MAKE_SYSTEM_ERROR(err),
            TStringBuilder() << "NO ID_SERIAL_SHORT value " << ::strerror(err));
    }
    return TString(value);
}

TResultOrError<bool> TSystemdNvmeManager::IsSsd(const TString& path)
{
    auto result = GetDevice(TFsPath(path));
    if (result.HasError()) {
        return result.GetError();
    }

    const char* value = nullptr;
    int res = sd_device_get_property_value(
        result.ExtractResult().get(),
        "ID_ATA_ROTATION_RATE_RPM",
        &value);

    return res < 0;
}

}   // namespace NCloud::NBlockStore::NNvme
