#include "dir_state.h"

#include <library/cpp/json/writer/json.h>

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

NJson::TJsonValue AttrsToJson(const TFileAttrs& attrs)
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["ino"] = attrs.Ino;
    json["mode"] = attrs.Mode;
    json["size"] = attrs.Size;
    json["links"] = attrs.Links;
    json["mtimeNs"] = attrs.MTimeNs;
    json["ctimeNs"] = attrs.CTimeNs;
    return json;
}

bool AttrsFromJson(const NJson::TJsonValue& json, TFileAttrs& attrs)
{
    constexpr const char* Keys[] =
        {"ino", "mode", "size", "links", "mtimeNs", "ctimeNs"};
    for (const auto* key: Keys) {
        if (!json[key].IsUInteger()) {
            return false;
        }
    }

    attrs = TFileAttrs{
        .Ino = json["ino"].GetUInteger(),
        .Mode = static_cast<ui32>(json["mode"].GetUInteger()),
        .Size = json["size"].GetUInteger(),
        .Links = json["links"].GetUInteger(),
        .MTimeNs = json["mtimeNs"].GetUInteger(),
        .CTimeNs = json["ctimeNs"].GetUInteger(),
    };
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString FormatAttrs(const TFileAttrs& attrs)
{
    NJsonWriter::TBuf buf;
    buf.BeginObject();
    buf.WriteKey("ino");
    buf.WriteULongLong(attrs.Ino);
    buf.WriteKey("mode");
    buf.WriteString(Sprintf("%o", attrs.Mode));
    buf.WriteKey("size");
    buf.WriteULongLong(attrs.Size);
    buf.WriteKey("links");
    buf.WriteULongLong(attrs.Links);
    buf.WriteKey("mtimeNs");
    buf.WriteULongLong(attrs.MTimeNs);
    buf.WriteKey("ctimeNs");
    buf.WriteULongLong(attrs.CTimeNs);
    buf.EndObject();
    return buf.Str();
}

NJson::TJsonValue TDirState::ToJson() const
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["instance"] = InstanceId;
    json["round"] = Round;
    json["dir"] = AttrsToJson(DirAttrs);

    auto& files = json["files"];
    files.SetType(NJson::JSON_MAP);
    for (const auto& [name, attrs]: Files) {
        files[name] = AttrsToJson(attrs);
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

    if (!AttrsFromJson(json["dir"], state.DirAttrs)) {
        return MakeError(E_ARGUMENT, "malformed dir attrs");
    }

    for (const auto& [name, file]: files.GetMapSafe()) {
        if (!AttrsFromJson(file, state.Files[name])) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "malformed attrs of " << name.Quote());
        }
    }

    if (!NamesFromJson(json["renamedFrom"], state.RenamedFrom) ||
        !NamesFromJson(json["unlinked"], state.Unlinked))
    {
        return MakeError(E_ARGUMENT, "malformed name lists");
    }

    return state;
}

}   // namespace NCloud::NFileStore
