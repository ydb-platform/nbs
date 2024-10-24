#include "manually_preempted_volumes.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/hash.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    TFile file(name, EOpenModeFlag::RdOnly);
    return TFileInput(file).ReadAll();
}

NProto::TError ReadFromFile(
    const TString& filePath,
    TManuallyPreemptedVolumes& volumes)
{
    if (filePath.empty()) {
        return {};
    }
    try {
        return volumes.Deserialize(ReadFile(filePath));
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder()
                << "Failed to read preempted volumes list with error: "
                << CurrentExceptionMessage());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TManuallyPreemptedVolumes::AddVolume(const TString& diskId, TInstant now)
{
    Volumes[diskId] = {now};
};

void TManuallyPreemptedVolumes::RemoveVolume(const TString& diskId)
{
    Volumes.erase(diskId);
}

void TManuallyPreemptedVolumes::ClearAll()
{
    Volumes.clear();
}

ui32 TManuallyPreemptedVolumes::GetSize() const
{
    return Volumes.size();
}

std::optional<TManuallyPreemptedVolumeInfo> TManuallyPreemptedVolumes::GetVolume(
    const TString& diskId) const
{
    if (auto it = Volumes.find(diskId); it != Volumes.end()) {
        return it->second;
    }
    return {};
}

TVector<TString> TManuallyPreemptedVolumes::GetVolumes() const
{
    TVector<TString> ans(Reserve(Volumes.size()));
    for (const auto& v: Volumes) {
        ans.emplace_back(v.first);
    }
    return ans;
}

TString TManuallyPreemptedVolumes::Serialize() const
{
    NJsonWriter::TBuf result;
    auto list = result
        .BeginObject()
        .WriteKey("Volumes")
        .BeginList();
    for (const auto& p: Volumes) {
        ui64 mcsUpdate = p.second.LastUpdate.MicroSeconds();
        list.BeginObject()
            .WriteKey("DiskId").WriteString(p.first)
            .WriteKey("Timestamp").WriteULongLong(mcsUpdate)
            .EndObject();

    };
    list.EndList().EndObject();
    return result.Str();
}

NProto::TError TManuallyPreemptedVolumes::Deserialize(const TString& input)
{
    try {
        if (input.empty()) {
            return {};
        }

        TStringInput in(input);
        auto tree = NJson::ReadJsonTree(&in, true);

        auto jsonVolumes = tree["Volumes"].GetArray();
        for (const auto& v: jsonVolumes) {
            AddVolume(
                v["DiskId"].GetStringSafe(),
                {TInstant::MicroSeconds(v["Timestamp"].GetUIntegerSafe())});
        }
        return {};
    } catch (...) {
        ClearAll();
        return MakeError(
            E_FAIL,
            TStringBuilder()
                << "Failed to load preempted volumes list with error: "
                << CurrentExceptionMessage());
    }
}

////////////////////////////////////////////////////////////////////////////////

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes(
    const TString& filePath,
    TLog& log,
    TVector<TString>& criticalEventsStorage)
{
    TManuallyPreemptedVolumesPtr result =
        std::make_shared<TManuallyPreemptedVolumes>();

    auto& Log = log;

    auto status = ReadFromFile(filePath, *result);

    if (FAILED(status.GetCode())) {
        criticalEventsStorage.emplace_back(
            GetCriticalEventForManuallyPreemptedVolumesFileError());
        STORAGE_ERROR(
            TStringBuilder()
                << "Failed to load manually preempted volumes: "
                << status.GetMessage());
        return result;
    }

    if (result->GetSize()) {
        TStringBuilder out;
        for (const auto& v: result->GetVolumes()) {
            out << ' ' << v;
        }

        STORAGE_INFO("Loaded manually preempted volumes:" << out);
    }

    return result;
}

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes()
{
    return std::make_shared<TManuallyPreemptedVolumes>();
}

TManuallyPreemptedVolumesPtr CreateManuallyPreemptedVolumes(
    const TStorageConfigPtr& storageConfig,
    TLog& log,
    TVector<TString>& criticalEventsStorage)
{
    return CreateManuallyPreemptedVolumes(
        !storageConfig->GetDisableManuallyPreemptedVolumesTracking() ?
            storageConfig->GetManuallyPreemptedVolumesFile() :
            "",
        log,
        criticalEventsStorage);
}

}   // namespace NCloud::NBlockStore::NStorage
