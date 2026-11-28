#include "immediate_control_board_actor.h"

#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/base/counters.h>
#include <contrib/ydb/library/services/services.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

using namespace NActors;

class TImmediateControlActor : public TActorBootstrapped<TImmediateControlActor> {
    struct TLogRecord {
        TInstant Timestamp;
        TString ParamName;
        TAtomicBase PrevValue;
        TAtomicBase NewValue;
        // Operator action label owned by this history record for its lifetime.
        TString Action;

        TLogRecord(TInstant timestamp, TString paramName, TAtomicBase prevValue, TAtomicBase newValue, TString action)
            : Timestamp(timestamp)
            , ParamName(paramName)
            , PrevValue(prevValue)
            , NewValue(newValue)
            , Action(action)
        {}

        TString TimestampToStr() {
            struct tm t_p;
            Timestamp.LocalTime(&t_p);
            return Sprintf("%4d-%02d-%02d %02d:%02d:%02d", (int)t_p.tm_year + 1900, (int)t_p.tm_mon + 1,
                    (int)t_p.tm_mday, (int)t_p.tm_hour, (int)t_p.tm_min, (int)t_p.tm_sec);
        }
    };

    TIntrusivePtr<TControlBoard> Board;
    TVector<TLogRecord> HistoryLog;

    ::NMonitoring::TDynamicCounters::TCounterPtr HasChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr ChangedCount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMMEDIATE_CONTROL_BOARD;
    }

    TImmediateControlActor(TIntrusivePtr<TControlBoard> board,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Board(board)
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> IcbGroup = GetServiceCounters(counters, "utils");
        HasChanged = IcbGroup->GetCounter("Icb/HasChangedContol");
        ChangedCount = IcbGroup->GetCounter("Icb/ChangedControlsCount");
    }


    void Bootstrap(const TActorContext &ctx) {
        auto mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "icb", "Immediate Control Board", false,
                    ctx.ActorSystem(), ctx.SelfID);
        }
        Become(&TThis::StateFunc);
    }

private:
    // Record an operator action only when it changes a control value.
    void RecordChange(const TString& name, TAtomicBase prevValue, TAtomicBase newValue, const TString& action) {
        if (prevValue != newValue) {
            HistoryLog.emplace_back(TInstant::Now(), name, prevValue, newValue, action);
        }
    }

    void HandlePostParams(const TCgiParameters &cgi) {
        if (cgi.Has("restoreDefault")) {
            const TString& controlName = cgi.Get("restoreDefault");
            TAtomicBase prevValue;
            TAtomicBase newValue;
            if (Board->RestoreDefault(controlName, prevValue, newValue)) {
                RecordChange(controlName, prevValue, newValue, "Restore default");
            }
            return;
        }
        if (cgi.Has("restoreDefaults")) {
            Board->RestoreDefaults();
            HistoryLog.emplace_back(TInstant::Now(), "RestoreDefaults", 0, 0, "Restore defaults");
            return;
        }
        for (const auto &param : cgi) {
            TAtomicBase newValue = strtoull(param.second.data(), nullptr, 10);
            TAtomicBase prevValue = newValue;
            Board->SetValue(param.first, newValue, prevValue);
            RecordChange(param.first, prevValue, newValue, "Set value");
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        HTTP_METHOD method = ev->Get()->Request.GetMethod();
        if (method == HTTP_METHOD_POST) {
            HandlePostParams(ev->Get()->Request.GetPostParams());
        }
        TStringStream str;
        ui64 changedCount;
        str << Board->RenderAsHtml(&changedCount);
        *ChangedCount = changedCount;
        *HasChanged = changedCount > 0;
        HTML(str) {
            str << "<h3>History</h3>";
            TABLE_SORTABLE_CLASS("historyLogTable") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Timestamp"; }
                        TABLEH() {str << "Parameter"; }
                        TABLEH() {str << "PrevValue"; }
                        TABLEH() {str << "NewValue"; }
                        TABLEH() {str << "Action"; }
                    }
                }
                TABLEBODY() {
                    for (auto &record : HistoryLog) {
                        TABLER() {
                            TABLED() { str << record.TimestampToStr(); }
                            TABLED() { str << record.ParamName; }
                            TABLED() { str << record.PrevValue; }
                            TABLED() { str << record.NewValue; }
                            TABLED() { str << record.Action; }
                        }
                    }
                }
            }
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    STFUNC(StateFunc) {
        switch(ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
        }
    }
};

NActors::IActor* CreateImmediateControlActor(TIntrusivePtr<TControlBoard> board,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new NKikimr::TImmediateControlActor(board, counters);
}
};
