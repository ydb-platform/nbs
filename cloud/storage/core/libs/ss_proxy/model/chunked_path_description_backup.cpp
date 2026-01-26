#include "chunked_path_description_backup.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NStorage::NSSProxy {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int ItemsPerChunkCount = 1000;
constexpr TStringBuf ChunkedProtoMagic = "CHKPROTO";
constexpr ui8 ChunkedProtoVersion = 1;

struct Y_PACKED TChunkedProtoFileHeader
{
    char Magic[8] = {};
    ui8 Version = ChunkedProtoVersion;
};

static_assert(
    sizeof(TChunkedProtoFileHeader::Magic) == ChunkedProtoMagic.size());
static_assert(
    sizeof(TChunkedProtoFileHeader) ==
    ChunkedProtoMagic.size() + sizeof(TChunkedProtoFileHeader::Version));

////////////////////////////////////////////////////////////////////////////////

void WriteHeader(IOutputStream* out)
{
    TChunkedProtoFileHeader header;
    memcpy(header.Magic, ChunkedProtoMagic.data(), ChunkedProtoMagic.size());

    out->Write(&header, sizeof(header));
}

bool LoadHeader(IInputStream* input, TChunkedProtoFileHeader* header)
{
    if (input->Read(header, sizeof(*header)) != sizeof(*header)) {
        return false;
    }

    if (TStringBuf{header->Magic, sizeof(header->Magic)} != ChunkedProtoMagic) {
        return false;
    }

    if (header->Version != ChunkedProtoVersion) {
        return false;
    }

    return true;
}

void SaveChunk(
    IOutputStream* out,
    const NSSProxy::NProto::TPathDescriptionBackup& chunk)
{
    if (chunk.GetData().empty()) {
        return;
    }

    const TString buffer = chunk.SerializeAsString();
    const ui32 chunkSize = buffer.size();
    out->Write(&chunkSize, sizeof(chunkSize));
    out->Write(buffer.data(), chunkSize);
}

NCloud::NProto::TError LoadChunk(
    IInputStream* input,
    NSSProxy::NProto::TPathDescriptionBackup* chunk)
{
    ui32 size = 0;
    if (input->Read(&size, sizeof(size)) != sizeof(size)) {
        return MakeError(
            size ? E_INVALID_STATE : S_FALSE,
            "Failed to read size");
    }

    TString chunkData;
    chunkData.resize(size);
    if (input->Read(const_cast<char*>(chunkData.data()), size) != size) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Failed to read chunk proto with size " << size);
    }
    if (!chunk->ParseFromString(chunkData)) {
        return MakeError(E_INVALID_STATE, "Failed to parse chunk proto");
    }

    return MakeError(S_OK);
}

}   // namespace

NCloud::NProto::TError SavePathDescriptionBackupToChunkedBinaryFormat(
    const TString& filePath,
    const NProto::TPathDescriptionBackup& pathDescriptionBackup)
{
    TOFStream out(filePath);

    // Write header
    WriteHeader(&out);

    // Write data
    NSSProxy::NProto::TPathDescriptionBackup chunk;
    auto& chunkData = *chunk.MutableData();
    for (const auto& [key, value]: pathDescriptionBackup.GetData()) {
        chunkData.insert({key, value});
        if (chunkData.size() >= ItemsPerChunkCount) {
            SaveChunk(&out, chunk);
            chunkData.clear();
        }
    }

    SaveChunk(&out, chunk);

    return MakeError(S_OK);
}

NCloud::NProto::TError LoadPathDescriptionBackupFromChunkedBinaryFormat(
    const TString& filePath,
    NProto::TPathDescriptionBackup* pathDescriptionBackup)
{
    auto& backupData = *pathDescriptionBackup->MutableData();

    TUnbufferedFileInput stream(filePath);

    TChunkedProtoFileHeader header;
    if (!LoadHeader(&stream, &header)) {
        return MakeError(E_INVALID_STATE, "Invalid header");
    }

    for (;;) {
        NSSProxy::NProto::TPathDescriptionBackup chunk;
        auto error = LoadChunk(&stream, &chunk);
        if (HasError(error)) {
            return error;
        }
        if (chunk.GetData().empty()) {
            break;
        }

        for (auto& [key, value]: *chunk.MutableData()) {
            backupData[key].Swap(&value);
        }
    }

    return MakeError(S_OK);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage::NSSProxy
