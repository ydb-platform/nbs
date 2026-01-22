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
#include <util/stream/null.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int ItemsPerChunkCount = 1000;
constexpr TStringBuf ChunkedProtoFileHeader = "CHKPROTO";

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

void SaveChunk(
    IOutputStream* out,
    const NSSProxy::NProto::TPathDescriptionBackupChunk& chunk)
{
    if (chunk.GetData().empty()) {
        return;
    }

    const TString buffer = chunk.SerializeAsString();
    const ui32 chunkSize = buffer.size();
    out->Write(&chunkSize, sizeof(chunkSize));
    out->Write(buffer.data(), chunkSize);
}

void DumpToChunkedProto(
    const TSchemeShardData& allData,
    const TFsPath& binaryOutputPath)
{
    if (!binaryOutputPath.GetPath()) {
        return;
    }

    Cout << "Dumping to " << binaryOutputPath.GetPath().Quote()
         << " with binary format, items count: " << allData.size() << Endl;

    TOFStream out(binaryOutputPath.GetPath());

    out.Write(ChunkedProtoFileHeader.data(), ChunkedProtoFileHeader.size());

    NSSProxy::NProto::TPathDescriptionBackupChunk chunk;
    auto& chunkData = *chunk.MutableData();
    for (const auto& [key, value]: allData) {
        NSSProxy::NProto::TPathDescriptionBackupItem item;
        item.Setkey(key);
        *item.Mutablevalue() = value;
        chunkData.Add(std::move(item));
        if (chunkData.size() >= ItemsPerChunkCount) {
            SaveChunk(&out, chunk);
            chunkData.Clear();
            Cout << ".";
        }
    }

    SaveChunk(&out, chunk);
    out.Flush();
    Cout << "OK" << Endl;
}

void DumpToTextProto(
    const TSchemeShardData& allData,
    const TFsPath& textOutputPath)
{
    if (!textOutputPath.GetPath()) {
        return;
    }

    Cout << "Dumping to " << textOutputPath.GetPath().Quote()
         << " with text format, items count: " << allData.size() << Endl;
    TFileOutput output(textOutputPath);
    for (const auto& [key, value]: allData) {
        NSSProxy::NProto::TPathDescriptionBackupChunk subBackup;
        NSSProxy::NProto::TPathDescriptionBackupItem item;
        item.Setkey(key);
        *item.Mutablevalue() = value;
        subBackup.MutableData()->Add(std::move(item));
        SerializeToTextFormat(subBackup, output);
    }
    Cout << "OK" << Endl;
}

void Run(const TOptions& options)
{
    TFsPath srcBackupsFilePath{options.SrcBackupsFilePath};

    TVector<TString> children;
    srcBackupsFilePath.ListNames(children);
    TSchemeShardData allData;
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
