#include <cloud/filestore/tools/analytics/profile_tool/lib/command.h>
#include <cloud/filestore/tools/analytics/profile_tool/lib/factory.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/singleton.h>
#include <util/string/join.h>
#include <util/system/progname.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TCommandPtr Command;

public:
    static TApp& Instance()
    {
        return *Singleton<TApp>();
    }

    int Run(int argc, const char** argv)
    {
        NLastGetopt::TOpts opts;
        opts.AddHelpOption('h');
        opts.AddVersionOption();
        opts.SetFreeArgsNum(1);

        opts.SetTitle("Command line NFS profile-log-tool");
        opts.SetFreeArgTitle(0, "<command>", JoinSeq(" | ", GetCommandNames()));

        try {
            if (argc < 2) {
                ythrow NLastGetopt::TUsageException() << "not enough arguments";
            }

            if (argc == 2) {
                TStringBuf arg(argv[1]);
                if (arg == "-h" || arg == "--help") {
                    opts.PrintUsage(GetProgramName());
                    return 0;
                } else if (arg == "-V" || arg == "--svnrevision") {
                    NLastGetopt::PrintVersionAndExit(nullptr);
                    return 0;
                }
            }

            auto name = NormalizeCommand(argv[1]);

            Command = GetCommand(name);
            if (!Command) {
                ythrow yexception() << "unknown command: " << name;
            }

            return Command->Run(argc, argv);

        } catch (const NLastGetopt::TUsageException& e) {
            Cerr << FormatCmdLine(argc, argv)
                 << " failed: " << CurrentExceptionMessage() << Endl;

            if (Command) {
                Command->GetOpts().PrintUsage(GetProgramName());
            } else {
                opts.PrintUsage(GetProgramName());
            }
            return 1;
        } catch (...) {
            Cerr << FormatCmdLine(argc, argv)
                 << " failed: " << CurrentExceptionMessage() << Endl;
            return 1;
        }
    }

private:
    static TString FormatCmdLine(int argc, const char** argv)
    {
        TStringBuilder result;
        result << GetProgramName();

        if (argc > 1) {
            result << " " << JoinRange(" ", &argv[1], &argv[argc - 1]).Quote();
        }
        return result;
    }
};

}   // namespace NCloud::NFileStore::NProfileTool

int main(int argc, const char** argv)
{
    return NCloud::NFileStore::NProfileTool::TApp::Instance().Run(argc, argv);
}
