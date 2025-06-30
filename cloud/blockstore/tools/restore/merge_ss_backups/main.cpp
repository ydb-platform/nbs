#include "options.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/hive_proxy/protos/tablet_boot_info_backup.pb.h>
#include <cloud/storage/core/libs/ss_proxy/protos/path_description_backup.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/stubs/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString SchemeShardBackup = "nbs-path-description-backup.txt";
const TString HiveBackup = "nbs-tablet-boot-info-backup.txt";

////////////////////////////////////////////////////////////////////////////////

struct TSchemeValue
{
    ui32 Generation = 0;
    ui64 PathVersion = 0;
    ui64 GeneralVersion = 0;
    ui64 BSVVersion = 0;
    ui64 VolumeTabletId = 0;
    NKikimrSchemeOp::TPathDescription Description;

    bool operator<(const TSchemeValue& other) const
    {
        auto doTie = [](const TSchemeValue& o)
        {
            return std::tie(
                o.Generation,
                o.PathVersion,
                o.GeneralVersion,
                o.BSVVersion);
        };

        return doTie(*this) < doTie(other);
    }

    TString Print() const
    {
        return TStringBuilder() << "Generation: " << Generation
                                << ", PathVersion: " << PathVersion
                                << ", GeneralVersion: " << GeneralVersion
                                << ", BSVVersion: " << BSVVersion
                                << ", VolumeTabletId: " << VolumeTabletId;
    }
};

using TSchemeShardData = TMap<TString, TSchemeValue>;

using THiveGeneration = TMap<ui64, ui32>;

////////////////////////////////////////////////////////////////////////////////

bool LoadSchemeShardBackup(
    const TFsPath& path,
    NSSProxy::NProto::TPathDescriptionBackup* backupProto)
{
    auto backupPath = path / SchemeShardBackup;
    if (!backupPath.Exists()) {
        return false;
    }
    TFile file(backupPath, OpenExisting | RdOnly | Seq);
    TUnbufferedFileInput input(file);
    NProtoBuf::LogSilencer silencer;
    return TryMergeFromTextFormat(
        input,
        *backupProto,
        EParseFromTextFormatOption::AllowUnknownField);
}

bool LoadHiveBackup(
    const TFsPath& path,
    NHiveProxy::NProto::TTabletBootInfoBackup* backupProto)
{
    auto backupPath = path / HiveBackup;
    if (!backupPath.Exists()) {
        return false;
    }
    TFile file(backupPath, OpenExisting | RdOnly | Seq);
    TUnbufferedFileInput input(file);
    NProtoBuf::LogSilencer silencer;
    return TryMergeFromTextFormat(
        input,
        *backupProto,
        EParseFromTextFormatOption::AllowUnknownField);
}

THiveGeneration GetTabletGeneration(
    const NHiveProxy::NProto::TTabletBootInfoBackup& backupProto)
{
    THiveGeneration result;
    for (const auto& [key, value]: backupProto.GetData()) {
        result[key] = value.GetSuggestedGeneration();
    }
    return result;
}

void ProcessDir(const TFsPath& path, TSchemeShardData* allData)
{
    const TInstant start = TInstant::Now();

    NSSProxy::NProto::TPathDescriptionBackup schemeProto;
    NHiveProxy::NProto::TTabletBootInfoBackup hiveProto;
    const bool ssOk = LoadSchemeShardBackup(path, &schemeProto);
    if (!ssOk) {
        Cout << path.GetPath().Quote() << " Failed" << Endl;
        return;
    }
    const bool hiveOk = LoadHiveBackup(path, &hiveProto);
    THiveGeneration tabletGeneration;
    if (hiveOk) {
        tabletGeneration = GetTabletGeneration(hiveProto);
    }
    auto getGeneration = [&](ui64 tabletId) -> ui32
    {
        if (auto* generation = tabletGeneration.FindPtr(tabletId)) {
            return *generation;
        }
        return 0;
    };

    for (auto& [key, value]: *schemeProto.MutableData()) {
        TSchemeValue newValue{
            .Generation = getGeneration(
                value.GetBlockStoreVolumeDescription().GetVolumeTabletId()),
            .PathVersion = value.GetSelf().GetPathVersion(),
            .GeneralVersion = value.GetSelf().GetVersion().GetGeneralVersion(),
            .BSVVersion = value.GetSelf().GetVersion().GetBSVVersion(),
            .VolumeTabletId =
                value.GetBlockStoreVolumeDescription().GetVolumeTabletId(),
            .Description = std::move(value)};

        auto* exist = allData->FindPtr(key);
        if (exist && *exist < newValue) {
            *exist = std::move(newValue);
        } else {
            allData->emplace(key, std::move(newValue));
        }
    }
    const TInstant end = TInstant::Now();
    Cout << path.GetPath().Quote()
         << " OK, file count: " << schemeProto.GetData().size()
         << ", total count: " << allData->size()
         << ", time: " << FormatDuration(end - start) << Endl;
}

void Dump(TSchemeShardData allData, const TFsPath& outputPath)
{
    TFileOutput output(outputPath);

    NSSProxy::NProto::TPathDescriptionBackup allProto;
    for (auto& [key, value]: allData) {
        (*allProto.MutableData())[key] = std::move(value.Description);
    }

    Cout << "Dumping to " << outputPath.GetPath().Quote()
         << " count: " << allData.size() << Endl;
    SerializeToTextFormat(allProto, output);
    Cout << "OK" << Endl;
}

void Run(const TOptions& options)
{
    TFsPath srcRoot{options.SrcRoot};

    TVector<TString> children;
    srcRoot.ListNames(children);
    TSchemeShardData allData;
    for (const auto& child: children) {
        ProcessDir(srcRoot / child, &allData);
    }
    Dump(std::move(allData), options.OutputPath);
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
