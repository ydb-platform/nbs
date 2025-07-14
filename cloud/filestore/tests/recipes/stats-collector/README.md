### stats-collector

Include this recipe to get a process that regularly collects stats from a given monitoring endpoint

Arguments:
- `FILESTORE_STATS_COLLECTOR_PERIOD` - The period in seconds at which the stats collector will run.
- `FILESTORE_STATS_COLLECTOR_ENDPOINT`  - The endpoint to collect stats from. Something like `http://localhost:8768/counters`.
