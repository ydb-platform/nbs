# Run eternal test

### Description
Run eternal load

### Usage

Build `eternal-load` binary

### How to run
Options:  \
`--config-type` - supported two config types:  \
    `file` - reads test config from json-file specified in `restore-config-path`  \
    `generated` - use auto generated test config with specified parameters  \
`--restore-config-path` - specify json-file with config  \
`--file` - path to file or block device  \
`--filesize` - size of block device or file in GB  \
`--iodepth` - number of load threads (only for new test)  \
`--blocksize` - block size of device in Bytes  \
`--request-block-count` - number of blocks per request  \
`--write-rate` - rate of write requests in percentage  \
`--dump-config-path` - specify in which file store config after test stops  \
Examples:
```(bash)
$ ./eternal-load --config-type generated  --blocksize 4096 --file /dev/vdb --filesize 1023 --iodepth 32 --dump-config-path /tmp/load-config.json --write-rate 50
$ ./eternal-load --config-type file --file /dev/vdb --dump-config-path /tmp/load-config.json --write-rate 50 --restore-config-path /tmp/load-config.json
```
