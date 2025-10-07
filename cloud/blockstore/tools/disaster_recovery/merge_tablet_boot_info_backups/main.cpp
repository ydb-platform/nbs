#include "options.h"

#include <cloud/storage/core/libs/common/format.h>
#include "cloud/storage/core/libs/hive_proxy/protos/tablet_boot_info_backup.pb.h"

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool operator<(
    const NHiveProxy::NProto::TTabletBootInfo& lhs,
    const NHiveProxy::NProto::TTabletBootInfo& rhs)
{
    return lhs.GetSuggestedGeneration() < rhs.GetSuggestedGeneration();
}

using THiveProxyData = TMap<ui64, NHiveProxy::NProto::TTabletBootInfo>;

////////////////////////////////////////////////////////////////////////////////

bool LoadTabletBootInfoBackup(
    const TOptions& options,
    const TFsPath& path,
    NHiveProxy::NProto::TTabletBootInfoBackup* backupProto)
{
    auto backupPath = path / options.TabletBootInfoBackupFileName;
    if (!backupPath.Exists()) {
        return false;
    }

    TFile file(backupPath, OpenExisting | RdOnly | Seq);
    TUnbufferedFileInput input(file);

    return TryMergeFromTextFormat(
               input,
               *backupProto,
               EParseFromTextFormatOption::AllowUnknownField) ||
           backupProto->MergeFromString(input.ReadAll());
}

void ProcessDir(
    const TOptions& options,
    const TFsPath& path,
    THiveProxyData* allData,
    size_t dirIndex,
    size_t totalDirCount)
{
    const TInstant start = TInstant::Now();

    NHiveProxy::NProto::TTabletBootInfoBackup tabletBootInfoProto;

    if (!LoadTabletBootInfoBackup(options, path, &tabletBootInfoProto)) {
        Cerr << path.GetPath().Quote() << " Failed" << Endl;
        return;
    }

    for (auto& [key, value]: *tabletBootInfoProto.MutableData()) {
        auto* exist = allData->FindPtr(key);
        if (exist) {
            if (*exist < value) {
                *exist = std::move(value);
            }
        } else {
            allData->emplace(key, std::move(value));
        }
    }

    Cout << dirIndex << "/" << totalDirCount << " " << path.GetPath().Quote()
         << " OK, file count: " << tabletBootInfoProto.GetData().size()
         << ", total count: " << allData->size()
         << ", time: " << FormatDuration(TInstant::Now() - start) << Endl;
}

void Dump(
    THiveProxyData allData,
    const TFsPath& textOutputPath,
    const TFsPath& binaryOutputPath)
{
    NHiveProxy::NProto::TTabletBootInfoBackup allProto;
    for (auto& [key, value]: allData) {
        (*allProto.MutableData())[key] = std::move(value);
    }

    if (textOutputPath.GetPath()) {
        Cout << "Dumping to " << textOutputPath.GetPath().Quote()
             << " with text format, items count: " << allData.size() << Endl;
        TFileOutput output(textOutputPath);
        SerializeToTextFormat(allProto, output);
        Cout << "OK" << Endl;
    }

    if (binaryOutputPath.GetPath()) {
        Cout << "Dumping to " << binaryOutputPath.GetPath().Quote()
             << " with binary format, items count: " << allData.size() << Endl;
        TOFStream out(binaryOutputPath.GetPath());
        allProto.SerializeToArcadiaStream(&out);
        Cout << "OK" << Endl;
    }
}

void Run(const TOptions& options)
{
    TFsPath srcBackupsFilePath{options.SrcBackupsFilePath};

    TVector<TString> children;
    srcBackupsFilePath.ListNames(children);
    THiveProxyData allData;
    size_t dirIndex = 0;
    for (const auto& child: children) {
        ProcessDir(
            options,
            srcBackupsFilePath / child,
            &allData,
            ++dirIndex,
            children.size());
    }
    Dump(std::move(allData), options.TextOutputPath, options.BinaryOutputPath);
}

}   // namespace

}   // namespace NCloud::NStorage

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    try {
        const TOptions options{argc, argv};

        Run(options);

    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
