#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

int IterateEventLogInner(
    IEventFactory* fac,
    IEventProcessor* proc,
    TString* outputFilename,
    int argc,
    const char** argv)
{
    class TProxy: public ITunableEventProcessor
    {
    public:
        TProxy(IEventProcessor* proc, TString* outputFilename)
            : Processor(proc), OutputFilename(outputFilename)
        {
        }

        void AddOptions(NLastGetopt::TOpts& opts) override
        {
            opts.AddLongOption("output-binary-log-file",
                "Enables output to the specified file, original binary "
                "format is preserved")
                .Optional()
                .StoreResult(OutputFilename);
        }

        void SetOptions(const TEvent::TOutputOptions& options) override
        {
            Processor->SetOptions(options);
        }

        void ProcessEvent(const TEvent* ev) override
        {
            Processor->ProcessEvent(ev);
        }

        bool CheckedProcessEvent(const TEvent* ev) override
        {
            return Processor->CheckedProcessEvent(ev);
        }

    private:
        IEventProcessor* Processor;
        TString* OutputFilename;
    };

    TProxy proxy(proc, outputFilename);
    return IterateEventLog(
        fac, static_cast<ITunableEventProcessor*>(&proxy), argc, argv);
}

int main(int argc, const char** argv)
{
    struct TEventProcessor
        : TProtobufEventProcessor
    {
        TEventLogPtr EventLog;
        TString OutputFilename;

        void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
        {
            auto* message =
                dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
            if (!message) {
                return;
            }

            if (OutputFilename) {
                if (!EventLog) {
                    EventLog = new TEventLog(
                        OutputFilename,
                        NEvClass::Factory()->CurrentFormat()
                    );
                }
                EventLog->LogEvent(*message);
                return;
            }

            auto order = GetItemOrder(*message);
            for (const auto& i: order) {
                switch (i.Type) {
                    case EItemType::Request: {
                        DumpRequest(*message, i.Index, out);
                        break;
                    }
                    case EItemType::BlockInfo: {
                        DumpBlockInfoList(*message, i.Index, out);
                        break;
                    }
                    case EItemType::BlockCommitId: {
                        DumpBlockCommitIdList(*message, i.Index, out);
                        break;
                    }
                    case EItemType::BlobUpdate: {
                        DumpBlobUpdateList(*message, i.Index, out);
                        break;
                    }
                    default: {
                        Y_ABORT("unknown item");
                    }
                }
            }
        }
    } processor;

    return IterateEventLogInner(
        NEvClass::Factory(),
        &processor,
        &processor.OutputFilename,
        argc,
        argv
    );
}
