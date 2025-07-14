#include "options.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/ss_proxy/protos/path_description_backup.pb.h>

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
    const NKikimrSchemeOp::TPathDescription& lhs,
    const NKikimrSchemeOp::TPathDescription& rhs)
{
    return lhs.GetSelf().GetPathVersion() < rhs.GetSelf().GetPathVersion();
}

using TSchemeShardData = TMap<TString, NKikimrSchemeOp::TPathDescription>;

////////////////////////////////////////////////////////////////////////////////

bool LoadPathDescriptionBackup(
    const TOptions& options,
    const TFsPath& path,
    NSSProxy::NProto::TPathDescriptionBackup* backupProto)
{
    auto backupPath = path / options.PathDescriptionBackupFileName;
    if (!backupPath.Exists()) {
        return false;
    }

    TFile file(backupPath, OpenExisting | RdOnly | Seq);
    TUnbufferedFileInput input(file);
    return TryMergeFromTextFormat(
        input,
        *backupProto,
        EParseFromTextFormatOption::AllowUnknownField);
}

void ProcessDir(
    const TOptions& options,
    const TFsPath& path,
    TSchemeShardData* allData,
    size_t dirIndex,
    size_t totalDirCount)
{
    const TInstant start = TInstant::Now();

    NSSProxy::NProto::TPathDescriptionBackup pathDescriptionProto;

    if (!LoadPathDescriptionBackup(options, path, &pathDescriptionProto)) {
        Cerr << path.GetPath().Quote() << " Failed" << Endl;
        return;
    }

    for (auto& [key, value]: *pathDescriptionProto.MutableData()) {
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
         << " OK, file count: " << pathDescriptionProto.GetData().size()
         << ", total count: " << allData->size()
         << ", time: " << FormatDuration(TInstant::Now() - start) << Endl;
}

void Dump(
    TSchemeShardData allData,
    const TFsPath& textOutputPath,
    const TFsPath& binaryOutputPath)
{
    NSSProxy::NProto::TPathDescriptionBackup allProto;
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
    TFsPath srcRoot{options.SrcRoot};

    TVector<TString> children;
    srcRoot.ListNames(children);
    TSchemeShardData allData;
    size_t dirIndex = 0;
    for (const auto& child: children) {
        ProcessDir(
            options,
            srcRoot / child,
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
