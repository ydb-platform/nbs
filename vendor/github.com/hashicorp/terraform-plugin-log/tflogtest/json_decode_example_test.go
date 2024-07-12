package tflogtest

import (
    "bytes"
    "context"
    "fmt"

    "github.com/hashicorp/terraform-plugin-log/tflog"
)

func ExampleMultilineJSONDecode() {
    var output bytes.Buffer

    ctx := RootLogger(context.Background(), &output)

    // Root provider logger is now available for usage, such as writing
    // entries, calling SetField(), or calling NewSubsystem().
    tflog.Trace(ctx, "entry 1")
    tflog.Trace(ctx, "entry 2")

    entries, err := MultilineJSONDecode(&output)

    if err != nil {
        // Typical unit testing would call t.Fatalf() here.
        fmt.Printf("unable to read multiple line JSON: %s", err)
    }

    // Entries can be checked via go-cmp's cmp.Diff() or other testing methods.
    // This example outputs them to stdout in an explicitly formatted string,
    // which would not be expected in typical unit testing.
    for _, entry := range entries {
        fmt.Printf("@message: %s\n", entry["@message"])
    }

    // Output:
    // @message: entry 1
    // @message: entry 2
}
