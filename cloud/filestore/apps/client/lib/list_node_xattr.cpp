#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListNodeXAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;

public:
    TListNodeXAttrCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .RequiredArgument("NODE_ID")
            .StoreResult(&NodeId);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto request = CreateRequest<NProto::TListNodeXAttrRequest>();
        request->SetNodeId(NodeId);

        auto response = WaitFor(
            session.ListNodeXAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);

        //
        // Values, when present, run parallel to Names. Xattr values are
        // arbitrary bytes; base64 keeps the JSON well-formed for
        // non-UTF-8 values.
        //

        const auto& names = response.GetNames();
        const auto& values = response.GetValues();

        if (JsonOutput) {
            NJson::TJsonValue namesJson(NJson::JSON_ARRAY);
            for (const auto& name: names) {
                namesJson.AppendValue(name);
            }
            NJson::TJsonValue valuesJson(NJson::JSON_ARRAY);
            for (const auto& value: values) {
                valuesJson.AppendValue(Base64Encode(value));
            }
            NJson::TJsonValue json;
            json.InsertValue("Names", std::move(namesJson));
            json.InsertValue("ValuesBase64", std::move(valuesJson));
            Cout << NJson::WriteJson(json) << Endl;
        } else {
            for (int i = 0; i < names.size(); ++i) {
                Cout << names[i];
                if (i < values.size()) {
                    Cout << "=" << values[i];
                }
                Cout << Endl;
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListNodeXAttrCommand()
{
    return std::make_shared<TListNodeXAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
