namespace NCloud::NBlockStore;

struct TRangeConfig
{
    StartOffset: ui64;
    RequestBlockCount: ui64;
    RequestCount: ui64;
    Step: ui64;
    StartBlockIdx: ui64;
    LastBlockIdx: ui64;
    NumberToWrite: ui64;
    WriteParts: ui64;
};

struct TTestConfigDesc
{
    TestId (required): ui64;
    FilePath (required): string;
    IoDepth (required): ui16;
    FileSize (required): ui64;
    BlockSize (required): ui64;
    WriteRate (required): ui16;
    RangeBlockCount: ui64;
    Ranges: [TRangeConfig];
    SlowRequestThreshold: string (default = "10s");
};
