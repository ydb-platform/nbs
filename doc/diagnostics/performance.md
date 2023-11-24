# Metrics used to analyze performance

## Performance guarantee match metrics

NBS has metrics that tell whether a specific disk matches some performance guarantees configured in the diagnostics config:
https://github.com/ydb-platform/nbs/blob/40a48140c5a0f9a74cce4509721ffdcd5bb23a62/cloud/blockstore/config/diagnostics.proto#L52

If a disk isn't processing the specified load AND doesn't provide the latency that is good enough to process the specified load at iodepth=32, these metrics will be nonzero, otherwise - zero.
Per-disk metrics (service=server_volume):
* Suffer - calculates the number of 1 second intervals within the last 15 second interval during which the guarantees were not met, takes values from 0 to 15
* SmoothSuffer - shows whether the disk meets performance guarantees in the last 15 second interval or not, values - 0 (meets guarantees) and 1 (guarantees not met)
* CriticalSuffer - the same as SmoothSuffer but the configured iops and throughput values are divided by CriticalFactor (default=2)

Aggregate metrics:
* DisksSuffer - aggregate for Suffer
* SmoothDisksSuffer - aggregate for SmoothSuffer
* StrictSLADisksSuffer - aggregate for SmoothSuffer for the disks belonging to the clouds specified in ```TDiagnosticsConfig::CloudIdsWithStrictSLA```
* CriticalDisksSuffer - aggregate for CriticalSuffer
* DownDisks - shows the number of disks with p100 of their ExecutionTime above ```TDiagnosticsConfig::*DowntimeThreshold``` during the last 15 second interval
