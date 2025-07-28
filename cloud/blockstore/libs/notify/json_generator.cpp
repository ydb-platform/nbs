#include "json_generator.h"

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept TDiskEvent =
    std::is_same_v<T, TDiskError> || std::is_same_v<T, TDiskBackOnline>;

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetTemplateId(const TDiskError&)
{
    return "nbs.nonrepl.error";
}

TStringBuf GetTemplateId(const TDiskBackOnline&)
{
    return "nbs.nonrepl.back-online";
}

////////////////////////////////////////////////////////////////////////////////

void OutputEvent(IOutputStream& out, const TDiskEvent auto& event)
{
    out << GetTemplateId(event) << " { " << event.DiskId << " }";
}

////////////////////////////////////////////////////////////////////////////////

void FillData(const TDiskEvent auto& event, NJson::TJsonValue& data)
{
    data["diskId"] = event.DiskId;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonMap TJsonGenerator::Generate(const TNotification& data)
{
    // TODO: Add Timestamp when time formatting will be supported
    // by Cloud Notify service
    NJson::TJsonMap v{
        {"type",
         std::visit(
             [](const auto& event) { return GetTemplateId(event); },
             data.Event)},
        {"data",
         NJson::TJsonMap{
             {"cloudId", data.CloudId},
             {"folderId", data.FolderId},
         }}};

    if (!data.UserId.empty()) {
        v["userId"] = data.UserId;
    } else {
        v["cloudId"] = data.CloudId;
    }

    std::visit([&v](const auto& e) { FillData(e, v["data"]); }, data.Event);

    return v;
}

}   // namespace NCloud::NBlockStore::NNotify

Y_DECLARE_OUT_SPEC(, NCloud::NBlockStore::NNotify::TEvent, out, event)
{
    using namespace NCloud::NBlockStore::NNotify;

    std::visit([&](const auto& e) { OutputEvent(out, e); }, event);
}
