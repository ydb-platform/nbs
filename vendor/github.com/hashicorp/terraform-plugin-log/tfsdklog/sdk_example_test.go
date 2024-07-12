package tfsdklog

import (
    "context"
    "os"
    "regexp"

    "github.com/hashicorp/go-hclog"
    "github.com/hashicorp/terraform-plugin-log/internal/logging"
)

func getExampleContext() context.Context {
    return NewRootSDKLogger(context.Background(), logging.WithOutput(os.Stdout),
        WithLevel(hclog.Trace), WithoutLocation(), logging.WithoutTimestamp())
}

func ExampleSetField() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    derivedCtx := SetField(exampleCtx, "foo", 123)

    // all messages logged with derivedCtx will now have foo=123
    // automatically included
    Trace(derivedCtx, "example log message")

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk","foo":123}
}

func ExampleTrace() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    Trace(exampleCtx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"trace","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}

func ExampleDebug() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    Debug(exampleCtx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"debug","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}

func ExampleInfo() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    Info(exampleCtx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"info","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}

func ExampleWarn() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    Warn(exampleCtx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"warn","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}

func ExampleError() {
    // this function calls New with the options it needs to be reliably
    // tested. Framework and SDK developers should call New, inject the
    // resulting context in their framework, and then pass it around. This
    // exampleCtx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    Error(exampleCtx, "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"error","@message":"hello, world","@module":"sdk","colors":["red","blue","green"],"foo":123}
}

func ExampleMaskFieldValuesWithFieldKeys() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskFieldValuesWithFieldKeys(exampleCtx, "field1")

    // all messages logged with exampleCtx will now have field1=***
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "field1": 123,
        "field2": 456,
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk","field1":"***","field2":456}
}

func ExampleMaskAllFieldValuesRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskAllFieldValuesRegexes(exampleCtx, regexp.MustCompile("v1|v2"))

    // all messages logged with exampleCtx will now have field values matching
    // the above regular expressions, replaced with ***
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleMaskAllFieldValuesStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskAllFieldValuesStrings(exampleCtx, "v1", "v2")

    // all messages logged with exampleCtx will now have field values equal
    // the above strings, replaced with ***
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleMaskMessageStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskMessageStrings(exampleCtx, "my-sensitive-data")

    // all messages logged with exampleCtx will now have my-sensitive-data
    // replaced with ***
    Trace(exampleCtx, "example log message with my-sensitive-data masked")

    // Output:
    // {"@level":"trace","@message":"example log message with *** masked","@module":"sdk"}
}

func ExampleMaskMessageRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskMessageRegexes(exampleCtx, regexp.MustCompile(`[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}`))

    // all messages logged with exampleCtx will now have strings matching the
    // regular expression replaced with ***
    Trace(exampleCtx, "example log message with 1234-1234-1234-1234 masked")

    // Output:
    // {"@level":"trace","@message":"example log message with *** masked","@module":"sdk"}
}

func ExampleOmitLogWithFieldKeys() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = OmitLogWithFieldKeys(exampleCtx, "field1")

    // all messages logged with exampleCtx and using field1 will be omitted
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "field1": 123,
        "field2": 456,
    })

    // Output:
    //
}

func ExampleOmitLogWithMessageStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = OmitLogWithMessageStrings(exampleCtx, "my-sensitive-data")

    // all messages logged with exampleCtx will now have my-sensitive-data
    // entries omitted
    Trace(exampleCtx, "example log message with my-sensitive-data masked")

    // Output:
    //
}

func ExampleOmitLogWithMessageRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = OmitLogWithMessageRegexes(exampleCtx, regexp.MustCompile(`[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}`))

    // all messages logged with exampleCtx will be omitted if they have a
    // string matching the regular expression
    Trace(exampleCtx, "example log message with 1234-1234-1234-1234 masked")

    // Output:
    //
}

func ExampleMaskLogRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskLogRegexes(exampleCtx, regexp.MustCompile("v1|v2"), regexp.MustCompile("message"))

    // all messages logged with exampleCtx will now have message content
    // and field values matching the above regular expressions, replaced with ***
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log ***","@module":"sdk","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleMaskLogStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = MaskLogStrings(exampleCtx, "v1", "v2", "message")

    // all messages logged with exampleCtx will now have message content
    // and field values equal the above strings, replaced with ***
    Trace(exampleCtx, "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log ***","@module":"sdk","k1":"*** plus some text","k2":"*** plus more text"}
}
