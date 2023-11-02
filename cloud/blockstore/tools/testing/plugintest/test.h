#pragma once

#include "private.h"

#include <cloud/vm/api/blockstore-plugin.h>

#include <cloud/blockstore/tools/testing/plugintest/test.pb.h>

#include <util/generic/string.h>
#include <util/system/dynlib.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TPluginTest
{
private:
    const TString Path;
    const TString Options;
    const TString EndpointFolder;

    BlockPluginHost PluginHost;

    TDynamicLibrary PluginLib;
    BlockPlugin_GetPlugin_t GetPluginFunc = nullptr;
    BlockPlugin_PutPlugin_t PutPluginFunc = nullptr;
    BlockPlugin_GetVersion_t GetVersionFunc = nullptr;

    BlockPlugin* Plugin = nullptr;

public:
    TPluginTest(
        TString path,
        TString options,
        ui32 hostMajor,
        ui32 hostMinor,
        TString endpointFolder);

    void Start();
    void Stop();

    const char* GetVersion();

    void MountVolume(
        BlockPlugin_Volume* volume,
        const NProto::NTest::TMountVolumeRequest& request);

    void UnmountVolume(
        BlockPlugin_Volume* volume,
        const NProto::NTest::TUnmountVolumeRequest& request);

    void ReadBlocks(
        BlockPlugin_Volume* volume,
        const NProto::NTest::TReadBlocksRequest& request);

    void WriteBlocks(
        BlockPlugin_Volume* volume,
        const NProto::NTest::TWriteBlocksRequest& request);

    void ZeroBlocks(
        BlockPlugin_Volume* volume,
        const NProto::NTest::TZeroBlocksRequest& request);
};

}   // namespace NCloud::NBlockStore
