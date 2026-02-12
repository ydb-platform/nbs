#include <cloud/blockstore/libs/local_nvme/sysfs_helpers.h>
#include <cloud/blockstore/libs/nvme/nvme.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <functional>

using namespace std::chrono_literals;
using namespace NCloud;
using namespace NCloud::NBlockStore;
using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    const ISysFsPtr SysFs = CreateSysFs("/sys");

    TOpts Opts;

    ILoggingServicePtr Logging;
    TLog Log;

    NNvme::INvmeManagerPtr NVMe;

public:
    int HandleDescribe(int argc, const char** argv)
    {
        TString pciAddr;
        AddPCIAddrOption(pciAddr);

        ParseOpts(argc, argv);

        return Invoke(
            [&]
            {
                STORAGE_DEBUG("Describe " << pciAddr.Quote());

                TString json;
                google::protobuf::util::MessageToJsonString(
                    SysFs->GetNVMeDeviceFromPCIAddr(pciAddr),
                    &json);
                Cout << json;
            });
    }

    int HandleReset(int argc, const char** argv)
    {
        TString path;
        AddCtrlPathOption(path);

        ParseOpts(argc, argv);

        return Invoke(
            [&]
            {
                STORAGE_DEBUG(
                    "Reset " << path.Quote() << " to a single namespace");

                CheckError(NVMe->ResetToSingleNamespace(path));
            });
    }

    int HandleSanitize(int argc, const char** argv)
    {
        TString path;
        AddCtrlPathOption(path);

        ParseOpts(argc, argv);

        return Invoke(
            [&]
            {
                STORAGE_DEBUG("Sanitize " << path.Quote());
                CheckError(NVMe->Sanitize(path));

                for (;;) {
                    const auto& [r, error] = NVMe->GetSanitizeStatus(path);
                    CheckError(error);

                    STORAGE_DEBUG(
                        "Sanitize status: " << FormatError(r.Status)
                                            << " progress: " << r.Progress);

                    if (!HasError(r.Status)) {
                        break;
                    }

                    if (r.Status.GetCode() != E_TRY_AGAIN) {
                        CheckError(r.Status);
                    }

                    Sleep(100ms);
                }
            });
    }

    int HandleBind(int argc, const char** argv)
    {
        TString pciAddr;
        TString driverName;

        AddPCIAddrOption(pciAddr);
        AddDriverNameOption(driverName);

        ParseOpts(argc, argv);

        return Invoke(
            [&]
            {
                STORAGE_DEBUG(
                    "Bind " << pciAddr.Quote() << " to " << driverName);

                SysFs->BindPCIDeviceToDriver(pciAddr, driverName);
            });
    }

private:
    void ParseOpts(int argc, const char** argv)
    {
        ELogPriority logPriority = TLOG_ERR;

        Opts.SetFreeArgsNum(0);
        Opts.AddHelpOption('h');
        Opts.AddLongOption("verbose", "output level for diagnostics messages")
            .OptionalArgument("STR")
            .Handler1T<TString>(
                [&logPriority](const auto& s)
                { logPriority = s ? GetLogLevel(s).GetRef() : TLOG_DEBUG; });

        TOptsParseResult res(&Opts, argc, argv);

        Logging =
            CreateLoggingService("console", {.FiltrationLevel = logPriority});
        Log = Logging->CreateLog("NVME");

        NVMe = NNvme::CreateNvmeManager(Logging, 1min);
    }

    void AddCtrlPathOption(TString& path)
    {
        Opts.AddLongOption("path", "path to a nvme controller (eg. /dev/nvme0)")
            .StoreResult(&path)
            .Required()
            .RequiredArgument("PATH");
    }

    void AddDriverNameOption(TString& driverName)
    {
        Opts.AddLongOption("driver", "target driver (vfio-pci|nvme)")
            .StoreResult(&driverName)
            .Required()
            .RequiredArgument("STR");
    }

    void AddPCIAddrOption(TString& pciAddr)
    {
        Opts.AddLongOption("pci", "PCI address")
            .StoreResult(&pciAddr)
            .Required()
            .RequiredArgument("PCI");
    }

    template <typename F>
    int Invoke(F&& fn) noexcept
    {
        try {
            fn();
        } catch (...) {
            STORAGE_ERROR(CurrentExceptionMessage());
            return 1;
        }

        return 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TApp app;

    TModChooser mods;
    mods.AddMode(
        "describe",
        std::bind_front(&TApp::HandleDescribe, &app),
        "describe NVMe device by PCI address");
    mods.AddMode(
        "sanitize",
        std::bind_front(&TApp::HandleSanitize, &app),
        "Sanitize NVMe using Crypto Erace or Block Erase action");
    mods.AddMode(
        "reset",
        std::bind_front(&TApp::HandleReset, &app),
        "reset NVMe device to a single namespace");
    mods.AddMode(
        "bind",
        std::bind_front(&TApp::HandleBind, &app),
        "bind NVMe device to a driver (vfio-pci|nvme)");

    return mods.Run(argc, argv);
}
