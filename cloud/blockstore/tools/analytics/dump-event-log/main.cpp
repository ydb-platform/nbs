#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

using namespace NCloud::NBlockStore;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEventProcessor: TProtobufEventProcessor
{
    TEventLogPtr EventLog;
    TString OutputFilename;

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        const auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (!message) {
            return;
        }

        if (OutputFilename) {
            if (!EventLog) {
                EventLog = MakeIntrusive<TEventLog>(
                    OutputFilename,
                    NEvClass::Factory()->CurrentFormat());
            }
            EventLog->LogEvent(*message);
            return;
        }

        const TVector<TItemDescriptor> order = GetItemOrder(*message);
        for (const auto& [type, index]: order) {
            switch (type) {
                case EItemType::Request: {
                    DumpRequest(*message, index, out);
                    break;
                }
                case EItemType::BlockInfo: {
                    DumpBlockInfoList(*message, index, out);
                    break;
                }
                case EItemType::BlockCommitId: {
                    DumpBlockCommitIdList(*message, index, out);
                    break;
                }
                case EItemType::BlobUpdate: {
                    DumpBlobUpdateList(*message, index, out);
                    break;
                }
                default: {
                    Y_ABORT("unknown item");
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventProcessorProxy: public ITunableEventProcessor
{
private:
    IEventProcessor* Processor;
    TString* OutputFilename;

public:
    explicit TEventProcessorProxy(TEventProcessor* proc)
        : Processor(proc)
        , OutputFilename(&proc->OutputFilename)
    {}

    void AddOptions(NLastGetopt::TOpts& opts) override
    {
        opts.AddLongOption(
                "output-binary-log-file",
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TEventProcessor processor;
    TEventProcessorProxy proxy(&processor);

    return IterateEventLog(
        NEvClass::Factory(),
        static_cast<ITunableEventProcessor*>(&proxy),
        argc,
        argv);
}
