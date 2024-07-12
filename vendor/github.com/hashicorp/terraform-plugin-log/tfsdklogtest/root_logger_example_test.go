package tfsdklogtest

import (
    "bytes"
    "context"
    "fmt"

    "github.com/hashicorp/terraform-plugin-log/tfsdklog"
)

func ExampleRootLogger() {
    var output bytes.Buffer

    ctx := RootLogger(context.Background(), &output)

    // Root SDK logger is now available for usage, such as writing entries,
    // calling SetField(), or calling NewSubsystem().
    tfsdklog.Trace(ctx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    fmt.Println(output.String())

    // Output:
    // {"@level":"trace","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}
