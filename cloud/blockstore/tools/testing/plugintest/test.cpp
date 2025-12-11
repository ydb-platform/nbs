#include "test.h"

#include <cloud/blockstore/config/plugin.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/event.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

BlockPluginHost PluginHost;

////////////////////////////////////////////////////////////////////////////////

struct TCompletion
{
    BlockPlugin_Completion C{};

    TManualEvent Event;

    TCompletion()
    {
        C.custom_data = this;
    }

    void Wait(int result)
    {
        Y_ENSURE(result == BLOCK_PLUGIN_E_OK);

        Event.WaitI();

        Y_ENSURE(C.status != BP_COMPLETION_ERROR);
    }

    void Complete()
    {
        Event.Signal();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIOVector
{
    BlockPlugin_IOVector V{};
    BlockPlugin_Buffer B{};

    TString Data;

    TIOVector(ui32 blockSize, ui32 blocksCount, char fill = 0)
        : Data(blockSize * blocksCount, fill)
    {
        B.iov_base = Data.begin();
        B.iov_len = Data.size();

        V.iov = &B;
        V.niov = 1;
        V.size = Data.size();
    }
};

////////////////////////////////////////////////////////////////////////////////

int CompleteRequest(BlockPluginHost* host, BlockPlugin_Completion* completion)
{
    Y_UNUSED(host);

    static_cast<TCompletion*>(completion->custom_data)->Complete();
    return BLOCK_PLUGIN_E_OK;
}

void LogMessage(BlockPluginHost* host, const char* msg)
{
    Y_UNUSED(host);

    Cerr << msg << Endl;
}

void RunLengthOutput(size_t i, char c, size_t counter, IOutputStream& out)
{
    if (counter != i) {
        out << " ";
    }
    out << "(" << counter << "," << int(c) << ")";
}

void RunLengthEncode(const TString& s, IOutputStream& out)
{
    size_t counter = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        const auto c = s[i];
        if (i) {
            if (c == s[i - 1]) {
                ++counter;
            } else {
                RunLengthOutput(i, s[i - 1], counter, out);
                counter = 1;
            }
        } else {
            ++counter;
        }
    }

    if (counter) {
        RunLengthOutput(s.size(), s.back(), counter, out);
    }

    out << Endl;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPluginTest::TPluginTest(
    TString path,
    TString options,
    ui32 hostMajor,
    ui32 hostMinor,
    TString endpointFolder)
    : Path(std::move(path))
    , Options(std::move(options))
    , EndpointFolder(std::move(endpointFolder))
{
    Y_ASSERT(PluginHost.state == nullptr);

    PluginHost.magic = BLOCK_PLUGIN_HOST_MAGIC;
    if (!hostMajor && !hostMinor) {
        PluginHost.version_minor = BLOCK_PLUGIN_API_VERSION_MINOR;
        PluginHost.version_major = BLOCK_PLUGIN_API_VERSION_MAJOR;
    } else {
        PluginHost.version_minor = hostMinor;
        PluginHost.version_major = hostMajor;
    }
    PluginHost.state = this;
    PluginHost.complete_request = CompleteRequest;
    PluginHost.log_message = LogMessage;
    PluginHost.instance_id = "plugin_test";
}

TPluginTest::~TPluginTest()
{
    Y_ASSERT(PluginHost.state == this);
    PluginHost.state = nullptr;
}

void TPluginTest::Start()
{
    PluginLib.Open(Path.c_str(), RTLD_NOW);
    PluginLib.SetUnloadable(
        false);   // otherwise asan finds 3 8-byte memory leaks

    GetPluginFunc = (BlockPlugin_GetPlugin_t)PluginLib.Sym(
        BLOCK_PLUGIN_GET_PLUGIN_SYMBOL_NAME);

    PutPluginFunc = (BlockPlugin_PutPlugin_t)PluginLib.Sym(
        BLOCK_PLUGIN_PUT_PLUGIN_SYMBOL_NAME);

    GetVersionFunc = (BlockPlugin_GetVersion_t)PluginLib.Sym(
        BLOCK_PLUGIN_GET_VERSION_SYMBOL_NAME);

    Plugin = (*GetPluginFunc)(&PluginHost, Options.c_str());
}

void TPluginTest::Stop()
{
    (*PutPluginFunc)(Plugin);
}

const char* TPluginTest::GetVersion()
{
    return GetVersionFunc();
}

void TPluginTest::MountVolume(
    BlockPlugin_Volume* volume,
    const NProto::NTest::TMountVolumeRequest& request)
{
    TString volumeName;
    TFsPath configFile;

    if (request.GetEndpoint()) {
        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(request.GetDiskId());

        auto socketPath = EndpointFolder + "/" + request.GetEndpoint();
        mountConfig.SetUnixSocketPath(std::move(socketPath));

        if (request.GetUseConfigFile()) {
            configFile = "./test_mount_config.txt";
            SerializeToTextFormat(mountConfig, configFile.GetPath());
            volumeName = configFile.GetPath();
        } else {
            TStringStream mountConfigStr;
            SerializeToTextFormat(mountConfig, mountConfigStr);
            volumeName = mountConfigStr.Str().c_str();
        }
    } else {
        volumeName = request.GetDiskId();
    }

    BlockPlugin_MountOpts opts = {};
    opts.volume_name = volumeName.c_str();
    opts.mount_token = request.GetToken().c_str();
    opts.instance_id = request.GetInstanceId().c_str();
    opts.access_mode =
        static_cast<BlockPlugin_AccessMode>(request.GetVolumeAccessMode());
    opts.mount_mode =
        static_cast<BlockPlugin_MountMode>(request.GetVolumeMountMode());
    opts.mount_seq_number = request.GetMountSeqNumber();

    TCompletion completion;
    completion.Wait(Plugin->mount_async(Plugin, &opts, volume, &completion.C));

    configFile.DeleteIfExists();
}

void TPluginTest::UnmountVolume(
    BlockPlugin_Volume* volume,
    const NProto::NTest::TUnmountVolumeRequest& request)
{
    Y_UNUSED(request);

    TCompletion completion;
    completion.Wait(Plugin->umount_async(Plugin, volume, &completion.C));
}

void TPluginTest::ReadBlocks(
    BlockPlugin_Volume* volume,
    const NProto::NTest::TReadBlocksRequest& request)
{
    BlockPlugin_ReadBlocks r = {};
    r.header.type = BLOCK_PLUGIN_READ_BLOCKS;
    r.header.size = sizeof(r);
    r.start_index = request.GetStartIndex();
    r.blocks_count = request.GetBlocksCount();

    TIOVector iov(volume->block_size, r.blocks_count);
    r.bp_iov = &iov.V;

    TCompletion completion;
    completion.Wait(
        Plugin->submit_request(Plugin, volume, &r.header, &completion.C));

    RunLengthEncode(iov.Data, Cout);
}

void TPluginTest::WriteBlocks(
    BlockPlugin_Volume* volume,
    const NProto::NTest::TWriteBlocksRequest& request)
{
    BlockPlugin_WriteBlocks r;
    r.header.type = BLOCK_PLUGIN_WRITE_BLOCKS;
    r.header.size = sizeof(r);
    r.start_index = request.GetStartIndex();
    r.blocks_count = request.GetBlocksCount();

    TIOVector iov(volume->block_size, r.blocks_count, request.GetTag());
    r.bp_iov = &iov.V;

    TCompletion completion;
    completion.Wait(
        Plugin->submit_request(Plugin, volume, &r.header, &completion.C));
}

void TPluginTest::ZeroBlocks(
    BlockPlugin_Volume* volume,
    const NProto::NTest::TZeroBlocksRequest& request)
{
    BlockPlugin_ZeroBlocks r;
    r.header.type = BLOCK_PLUGIN_ZERO_BLOCKS;
    r.header.size = sizeof(r);
    r.start_index = request.GetStartIndex();
    r.blocks_count = request.GetBlocksCount();

    TCompletion completion;
    completion.Wait(
        Plugin->submit_request(Plugin, volume, &r.header, &completion.C));
}

}   // namespace NCloud::NBlockStore
