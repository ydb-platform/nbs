#include "dir_state.h"

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue NamesToJson(const TVector<TString>& names)
{
    NJson::TJsonValue json(NJson::JSON_ARRAY);
    for (const auto& name: names) {
        json.AppendValue(name);
    }
    return json;
}

bool NamesFromJson(const NJson::TJsonValue& json, TVector<TString>& names)
{
    if (!json.IsArray()) {
        return false;
    }

    for (const auto& name: json.GetArraySafe()) {
        if (!name.IsString()) {
            return false;
        }
        names.push_back(name.GetString());
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString FormatAttrs(const TFileAttrs& attrs)
{
    return TStringBuilder() << "{ino=" << attrs.Ino
        << ", mode=" << Sprintf("%o", attrs.Mode)
        << ", size=" << attrs.Size
        << ", links=" << attrs.Links << "}";
}

NJson::TJsonValue TDirState::ToJson() const
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["instance"] = InstanceId;
    json["round"] = Round;

    auto& files = json["files"];
    files.SetType(NJson::JSON_MAP);
    for (const auto& [name, attrs]: Files) {
        auto& file = files[name];
        file["ino"] = attrs.Ino;
        file["mode"] = attrs.Mode;
        file["size"] = attrs.Size;
        file["links"] = attrs.Links;
    }

    json["renamedFrom"] = NamesToJson(RenamedFrom);
    json["unlinked"] = NamesToJson(Unlinked);
    return json;
}

TResultOrError<TDirState> TDirState::FromJson(const NJson::TJsonValue& json)
{
    TDirState state;
    const auto& files = json["files"];
    if (!json["instance"].IsUInteger() || !json["round"].IsUInteger() ||
        !files.IsMap())
    {
        return MakeError(E_ARGUMENT, "malformed state");
    }

    state.InstanceId = json["instance"].GetUInteger();
    state.Round = json["round"].GetUInteger();

    for (const auto& [name, file]: files.GetMapSafe()) {
        if (!file["ino"].IsUInteger() || !file["mode"].IsUInteger() ||
            !file["size"].IsUInteger() || !file["links"].IsUInteger())
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "malformed attrs of " << name.Quote());
        }

        state.Files[name] = TFileAttrs{
            .Ino = file["ino"].GetUInteger(),
            .Mode = static_cast<ui32>(file["mode"].GetUInteger()),
            .Size = file["size"].GetUInteger(),
            .Links = file["links"].GetUInteger(),
        };
    }

    if (!NamesFromJson(json["renamedFrom"], state.RenamedFrom) ||
        !NamesFromJson(json["unlinked"], state.Unlinked))
    {
        return MakeError(E_ARGUMENT, "malformed name lists");
    }

    return state;
}

}   // namespace NCloud::NFileStore
