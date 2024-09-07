#include "online_request_monitor.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

namespace NCloud::NBlockStore::NStorage {

void TOnlineRequestMonitor::RequestStarted(ui64 requestId, TBlockRange64 range)
{
    Requests.emplace(requestId, TRequestMonInfo{range, TInstant::Now(), {}});
}

void TOnlineRequestMonitor::RequestFinished(ui64 requestId)
{
    if (auto* request = Requests.FindPtr(requestId)) {
        request->FinishAt = TInstant::Now();
    }
}

TString TOnlineRequestMonitor::TakeRequestInfo()
{
    const auto now = TInstant::Now();
    TStringStream out;
    NJson::TJsonValue json;

    NJson::TJsonValue requests(NJson::EJsonValueType::JSON_ARRAY);
    for (auto it = Requests.begin(); it != Requests.end();) {
        const auto& requestInfo = it->second;

        NJson::TJsonValue request(NJson::EJsonValueType::JSON_ARRAY);
        request.AppendValue(requestInfo.Range.Start);
        request.AppendValue(requestInfo.Range.Size());
        auto duration = requestInfo.FinishAt
                            ? requestInfo.FinishAt - requestInfo.StartAt
                            : now - requestInfo.StartAt;
        request.AppendValue(duration.MicroSeconds());
        requests.AppendValue(std::move(request));

        if (requestInfo.FinishAt) {
            Requests.erase(it++);
        } else {
            ++it;
        }
    }
    json["requests"] = std::move(requests);

    NJson::WriteJson(&out, &json);
    return out.Str();
}

}   // namespace NCloud::NBlockStore::NStorage
