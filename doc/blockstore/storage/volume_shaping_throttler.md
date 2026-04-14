# Volume shaping throttler (`TVolumeShapingThrottler`)

## General information

`TVolumeShapingThrottler` is a per-volume shaping mechanism that can introduce an extra delay after an IO request completes, so that the observable request completion rate matches a configured media-specific performance profile (IOPS/BW).

Like the regular volume throttler, shaping works on the volume tablet level, but unlike it shaping is intentionally **post-request**:
- the volume calculates the actual `executionTime` of the already executed request and passes it to the shaper,
- computes an expected “cost” for that request from the configured quota,
- returns the remaining delay that should still be applied.

Shaping is implemented in:
- `cloud/blockstore/libs/storage/volume/model/volume_shaping_throttler.{h,cpp}`
and is backed by a reusable bucket:
- `cloud/storage/core/libs/throttling/unspent_cost_bucket.{h,cpp}`

If the selected quota does not contain a corresponding performance profile (`Read` or `Write`), shaping for that op is also effectively disabled (returns zero delay).

Also, **small important note**: volume actor relies on the fact that if the shaping throttler is enabled then the traditional volume throttler is also enabled. The shaping throttler just would not work otherwise.

## Configuration and quota selection

Configuration is provided via `TShapingThrottlerConfig` / `TShapingThrottlerQuota` in:
- `cloud/blockstore/config/storage.proto`

`TVolumeShapingThrottler` selects the quota by media kind:
- `STORAGE_MEDIA_SSD` → `SsdQuota`
- `STORAGE_MEDIA_HDD`, `STORAGE_MEDIA_HYBRID` → `HddQuota`
- `STORAGE_MEDIA_SSD_NONREPLICATED` → `NonreplQuota`
- `STORAGE_MEDIA_SSD_MIRROR2` → `Mirror2Quota`
- `STORAGE_MEDIA_SSD_MIRROR3` → `Mirror3Quota`
- default → `HddQuota`

If the selected quota (`TShapingThrottlerQuota`) does not contain a corresponding performance profile (`Read` or `Write`), shaping for that op is also effectively disabled (returns zero delay).

## Expected cost model (`CostPerIO`)

For shaped requests, `SuggestDelay` computes an expected duration `cost` and passes it to the bucket along with the actual `executionTime`.

The per-IO “cost” is computed by formula:

$$ CostPerIO = ExpectedIoParallelism\left(\frac{1}{maxIops} + (\frac{byteCount}{maxBandwidth})\right) \text{seconds} $$

- `ExpectedIoParallelism` – is a config parameter.

## `TUnspentCostBucket`: how delay is derived from budget

`TUnspentCostBucket` is a generic helper for turning “unspent” execution time into a delay while smoothing behavior over time.

### Inputs

`Register(now, cost, spentCost)` receives:
- **`cost`**: expected duration of the operation
- **`spentCost`**: actual time already spent executing the operation

If `spentCost >= cost`, the request already took long enough and **no additional delay is needed** (`0` is returned).

Otherwise define the gap:
$ gap = cost - spentCost $

The bucket can cover part of this gap from its accumulated budget and returns the remainder.

### Budget spending and “smootherstep”

Spending aggressiveness depends on how full the bucket is:
- budget ratio $ r = CurrentBudget/MaxBudget $ is a number in \([0,1]\)
- a smooth curve is applied:
- about the function see https://en.wikipedia.org/wiki/Smoothstep

$ discountFactor = Smootherstep(r) = 6r^5 - 15r^4 + 10r^3 $

The bucket chooses how much of the gap it *wants* to cover between:

$ minBudgetSpend = cost * internalFactor - spentCost $
$ maxBudgetSpend = gap $

and interpolates between them with `discountFactor`. The actual covered amount is limited by the currently available budget.

### Budget refill

Bucket budget refills linearly with wall time and is capped:
- `MaxBudget`: capacity (clamped to at least 1ms internally)
- `BudgetRefillTime`: time to refill from 0 to `MaxBudget`

Refill rate is:

$
BudgetRefillRate = \frac{MaxBudget}{BudgetRefillTime}
$

If `BudgetRefillTime == 0`, refill rate is 0 (budget never refills).

## Persistence of shaping throttler state

The volume persists shaping state so that the bucket “fullness” survives tablet restarts.

### Periodic write

In `TVolumeActor::HandleUpdateThrottlerState`, the volume periodically writes current spent budget share into localdb as a part of `TVolumeDatabase::TThrottlerStateInfo`.

### Restore on volume tablet load

On startup, `TVolumeActor::CompleteLoadState` loads `SpentShapingBudgetShare` from localdb and passes it into `TVolumeState`, which constructs `TVolumeShapingThrottler(..., spentShapingBudgetShare)`.

## Observability

### Volume shaping quota
The shaping bucket “spent share” is exported as a percentage in volume self-counters:
- `simple.ShapingThrottlerBudgetSpent = round(100 * SpentShapingBudgetShare)`

### ShapingTime metrics
The `server` includes histogram for `ShapingTime`. The `server_volume` includes percentiles of `ShapingTime`. Both metrics together allow to monitor the effectiveness of shapers per-host and per-volume.
