#include "options.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/ss_proxy/model/chunked_path_description_backup.h>
#include <cloud/storage/core/libs/ss_proxy/protos/path_description_backup.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/null.h>
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
    const TString fileContent = TUnbufferedFileInput(file).ReadAll();
    auto input = TStringInput(fileContent);

    TNullOutput warningStream;
    return TryParseFromTextFormat(
               input,
               *backupProto,
               EParseFromTextFormatOption::AllowUnknownField,
               &warningStream) ||
           backupProto->MergeFromString(fileContent);
}

void ProcessDir(
    const TOptions& options,
    const TFsPath& path,
    NSSProxy::NProto::TPathDescriptionBackup* allData,
    size_t dirIndex,
    size_t totalDirCount)
{
    const TInstant start = TInstant::Now();

    NSSProxy::NProto::TPathDescriptionBackup pathDescriptionProto;

    if (!LoadPathDescriptionBackup(options, path, &pathDescriptionProto)) {
        Cerr << path.GetPath().Quote() << " Failed" << Endl;
        return;
    }

    auto& data = *allData->MutableData();
    for (auto& [key, value]: *pathDescriptionProto.MutableData()) {
        auto it = data.find(key);
        if (it != data.end()) {
            if (it->second < value) {
                it->second = std::move(value);
            }
        } else {
            data[key] = std::move(value);
        }
    }

    Cout << dirIndex << "/" << totalDirCount << " " << path.GetPath().Quote()
         << " OK, file count: " << pathDescriptionProto.GetData().size()
         << ", total count: " << data.size()
         << ", time: " << FormatDuration(TInstant::Now() - start) << Endl;
}

void DumpToTextProto(
    const NSSProxy::NProto::TPathDescriptionBackup& allData,
    const TFsPath& textOutputPath)
{
    if (!textOutputPath.GetPath()) {
        return;
    }

    Cout << "Dumping to " << textOutputPath.GetPath().Quote()
         << " with text format, items count: " << allData.GetData().size()
         << Endl;
    TFileOutput output(textOutputPath);
    SerializeToTextFormat(allData, output);
    Cout << "OK" << Endl;
}

void DumpToChunkedProto(
    const NSSProxy::NProto::TPathDescriptionBackup& allData,
    const TFsPath& binaryOutputPath)
{
    if (!binaryOutputPath.GetPath()) {
        return;
    }

    Cout << "Dumping to " << binaryOutputPath.GetPath().Quote()
         << " with binary format, items count: " << allData.GetData().size()
         << Endl;

    auto result = NSSProxy::SavePathDescriptionBackupToChunkedBinaryFormat(
        binaryOutputPath.GetPath(),
        allData);
    if (!HasError(result)) {
        Cout << "OK" << Endl;
    } else {
        Cout << FormatError(result) << Endl;
    }
}

void Run(const TOptions& options)
{
    TFsPath srcBackupsFilePath{options.SrcBackupsFilePath};

    TVector<TString> children;
    srcBackupsFilePath.ListNames(children);
    NSSProxy::NProto::TPathDescriptionBackup allData;
    size_t dirIndex = 0;
    for (const auto& child: children) {
        ProcessDir(
            options,
            srcBackupsFilePath / child,
            &allData,
            ++dirIndex,
            children.size());
    }

    DumpToTextProto(allData, options.TextOutputPath);
    DumpToChunkedProto(allData, options.BinaryOutputPath);
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
