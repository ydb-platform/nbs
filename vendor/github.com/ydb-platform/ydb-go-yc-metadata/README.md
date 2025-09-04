# ydb-go-yc-metadata

> helpers to connect to YDB inside yandex-cloud using metadata service  

[godoc](https://godoc.org/github.com/ydb-platform/ydb-go-yc-metadata/)

## Overview <a name="Overview"></a>

Currently package provides helpers to connect to YDB inside yandex-cloud.

## Usage <a name="Usage"></a>

```go
import (
	yc "github.com/ydb-platform/ydb-go-yc-metadata"
)
...
    db, err := ydb.Open(
        ctx,
	os.Getenv("YDB_CONNECTION_STRING"),
        yc.WithInternalCA(),
        yc.WithCredentials(ctx), // auth inside cloud (virtual machine or yandex function)
    )
    
```
