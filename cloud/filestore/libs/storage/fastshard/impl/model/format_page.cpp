#include "format_page.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

ui64 TFormatPage::Init(ui64 pageNo, IPageStorePtr pageStore)
{
    Slot.PageNo = pageNo;
    PageStore = std::move(pageStore);
    return 1;
}

NProto::TError TFormatPage::RegisterStart(
    ui32 minVersion,
    ui32 version,
    TStringBuf description,
    TWriteContext& writeContext)
{
    TBuffer page;
    auto error = PageStore->ReadPage(writeContext.Lsn, Slot.PageNo, &page);
    if (HasError(error)) {
        return error;
    }

    Y_ABORT_UNLESS(page.Size() == PageStore->GetPageSize());

    TFormatPageSlot slot;
    char* slotPtr = reinterpret_cast<char*>(&slot);
    memcpy(slotPtr, page.Data(), sizeof(slot));
    const bool isEmpty =
        *slotPtr == 0 && memcmp(slotPtr, slotPtr + 1, sizeof(slot) - 1) == 0;
    if (!isEmpty) {
        if (slot.PageNo != Slot.PageNo) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "unexpected PageNo in FormatPage: "
                    << slot.PageNo << " != " << Slot.PageNo);
        }

        if (slot.Version < minVersion) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "version too old: " << slot.Version
                    << " < " << minVersion);
        }

        if (version < slot.Version) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "version too new: " << version
                    << " < " << slot.Version);
        }
    }

    Slot.Generation = slot.Generation + 1;
    Slot.MinVersion = minVersion;
    Slot.Version = version;

    page.Clear();
    page.Resize(PageStore->GetPageSize());

    const ui64 actualDescriptionLen =
        Min(DescriptionCapacity - 1, description.Size());
    description.Trunc(actualDescriptionLen);
    memcpy(Slot.Description, description.Data(), actualDescriptionLen);
    memset(
        Slot.Description + actualDescriptionLen,
        0,
        DescriptionCapacity - actualDescriptionLen);
    memcpy(page.Data(), reinterpret_cast<char*>(&Slot), sizeof(Slot));

    return PageStore->WritePage(
        writeContext.Lsn,
        Slot.PageNo,
        page,
        writeContext.PageGroups);
}

[[nodiscard]] TString TFormatPage::Describe() const
{
    return TStringBuilder() << "G=" << Slot.Generation
        << " P=" << Slot.PageNo
        << " MV=" << Slot.MinVersion
        << " V=" << Slot.Version
        << " D={" << Slot.Description << "}";
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
