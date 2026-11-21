#include "immediate_control_board_impl.h"

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

bool TControlBoard::RegisterLocalControl(TControlWrapper control, TString name) {
    TIntrusivePtr<TControl> ptr;
    bool result = Board.Swap(name, control.Control, ptr);
    return !result;
}

bool TControlBoard::RegisterSharedControl(TControlWrapper& control, TString name) {
    TIntrusivePtr<TControl> ptr;
    if (Board.Get(name, ptr)) {
        control.Control = ptr;
        return false;
    }
    ptr = Board.InsertIfAbsent(name, control.Control);
    if (control.Control == ptr) {
        return true;
    } else {
        control.Control = ptr;
        return false;
    }
}

void TControlBoard::RestoreDefaults() {
    for (auto& bucket : Board.Buckets) {
        TReadGuard guard(bucket.GetLock());
        for (auto &control : bucket.GetMap()) {
            control.second->RestoreDefault();
        }
    }
}

void TControlBoard::RestoreDefault(TString name) {
    TAtomicBase prevValue;
    TAtomicBase newValue;
    RestoreDefault(name, prevValue, newValue);
}

// Restore one registered control and leave both outputs untouched for an unknown name.
bool TControlBoard::RestoreDefault(TString name, TAtomicBase& outPrevValue, TAtomicBase& outNewValue) {
    TIntrusivePtr<TControl> control;
    if (Board.Get(name, control)) {
        control->RestoreDefault(outPrevValue, outNewValue);
        return true;
    }
    return false;
}

bool TControlBoard::SetValue(TString name, TAtomic value, TAtomic &outPrevValue) {
    TIntrusivePtr<TControl> control;
    if (Board.Get(name, control)) {
        outPrevValue = control->SetFromHtmlRequest(value);
        return control->IsDefault();
    }
    return true;
}

// Only for tests
void TControlBoard::GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const {
    TIntrusivePtr<TControl> control;
    outIsControlExists = Board.Get(name, control);
    if (outIsControlExists) {
        outValue = control->Get();
    }
}

TString TControlBoard::RenderAsHtml(ui64* outChangedCount) const {
    TStringStream str;
    ui64 changedCount = 0;
    HTML(str) {
        TABLE_SORTABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Parameter"; }
                    TABLEH() { str << "Acceptable range"; }
                    TABLEH() { str << "Current"; }
                    TABLEH() { str << "Default"; }
                    TABLEH() { str << "Send new value"; }
                    TABLEH() { str << "Changed"; }
                }
            }
            TABLEBODY() {
                for (const auto& bucket : Board.Buckets) {
                    TReadGuard guard(bucket.GetLock());
                    for (const auto &item : bucket.GetMap()) {
                        const bool isDefault = item.second->IsDefault();
                        changedCount += !isDefault;
                        TABLER() {
                            TABLED() { str << item.first; }
                            TABLED() { str << item.second->RangeAsString(); }
                            TABLED() {
                                if (isDefault) {
                                    str << "<p>" << item.second->Get() << "</p>";
                                } else {
                                    str << "<p style='color:red;'><b>" << item.second->Get() << " </b></p>";
                                }
                            }
                            TABLED() {
                                if (isDefault) {
                                    str << "<p>" << item.second->GetDefault() << "</p>";
                                } else {
                                    str << "<p style='color:red;'><b>" << item.second->GetDefault() << " </b></p>";
                                }
                            }
                            TABLED() {
                                str << "<form class='form_horizontal' method='post'>";
                                str << "<input name='" << item.first << "' type='text' value='"
                                    << item.second->Get() << "'/>";
                                str << "<button type='submit' style='color:red;'><b>Change</b></button>";
                                if (!isDefault) {
                                    str << "<button type='submit' name='restoreDefault' value='" << item.first
                                        << "' style='color:green; margin-left:4px; white-space:nowrap;'>"
                                        << "<b>Restore Default</b></button>";
                                }
                                str << "</form>";
                            }
                            TABLED() { str << !isDefault; }
                        }
                    }
                }
            }
        }
        str << "<form class='form_horizontal' method='post'>";
        str << "<button type='submit' name='restoreDefaults' style='color:green;'><b>Restore Defaults</b></button>";
        str << "</form>";
    }
    if (outChangedCount) {
        *outChangedCount = changedCount;
    }
    return str.Str();
}

}
