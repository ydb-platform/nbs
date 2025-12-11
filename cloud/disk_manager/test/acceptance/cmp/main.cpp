#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/system/file.h>

////////////////////////////////////////////////////////////////////////////////

struct TParams
{
    bool Verbose = false;
    size_t Bytes = 0;
    size_t IgnoreInitial = 0;
    size_t ChunkSize = 4 * 1024 * 1024;
    int MaxRetriesAfterFailure = 5;
    TString File1;
    TString File2;
};

////////////////////////////////////////////////////////////////////////////////

int Run(const TParams& params)
{
    TLog log(CreateLogBackend("cout"));
    log.SetFormatter(
        [](ELogPriority, TStringBuf msg)
        {
            return TStringBuilder()
                   << "["
                   << TInstant::Now().FormatLocalTime("%Y-%m-%d %H:%M:%S")
                   << "] " << msg;
        });

    auto f1 = TFile(params.File1, RdOnly | DirectAligned);
    auto f2 = TFile(params.File2, RdOnly | DirectAligned);

    f1.Seek(params.IgnoreInitial, sSet);
    f2.Seek(params.IgnoreInitial, sSet);

    TVector<ui8> buf1(params.ChunkSize);
    TVector<ui8> buf2(params.ChunkSize);

    int retriesAfterFailure = 0;
    size_t offset = 0;
    while (offset < params.Bytes) {
        f1.Load(buf1.data(), buf1.size());
        f2.Load(buf2.data(), buf2.size());

        bool failed = false;
        for (size_t i = 0; i < buf1.size(); i++) {
            if (buf1[i] != buf2[i]) {
                log << offset + i << " " << (ui32)buf1[i] << " "
                    << (ui32)buf2[i] << Endl;
                failed = true;
            }
        }

        if (failed) {
            if (retriesAfterFailure < params.MaxRetriesAfterFailure) {
                retriesAfterFailure++;
                log << "files mismatch, retry " << retriesAfterFailure << Endl;

                f1.Seek(-params.ChunkSize, sCur);
                f2.Seek(-params.ChunkSize, sCur);
                continue;
            }

            log << "max number of retries after failure reached, abort" << Endl;
            return 1;
        }

        if (retriesAfterFailure > 0) {
            log << "retry after failure succeeded at offset " << offset << Endl;
            return 1;
        }

        offset += params.ChunkSize;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TParams params;

    NLastGetopt::TOpts opts;
    opts.AddLongOption("verbose").NoArgument().StoreResult(&params.Verbose);
    opts.AddLongOption("bytes").StoreResult(&params.Bytes);
    opts.AddLongOption("ignore-initial").StoreResult(&params.IgnoreInitial);
    opts.AddLongOption("chunk-size").StoreResult(&params.ChunkSize);
    opts.AddLongOption("max-retries-after-failure")
        .StoreResult(&params.MaxRetriesAfterFailure);
    opts.AddFreeArgBinding("file1", params.File1);
    opts.AddFreeArgBinding("file2", params.File2);
    opts.SetFreeArgsMax(2);

    NLastGetopt::TOptsParseResult r(&opts, argc, argv);

    return Run(params);
}
