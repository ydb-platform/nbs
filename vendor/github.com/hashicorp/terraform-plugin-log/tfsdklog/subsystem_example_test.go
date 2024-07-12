package tfsdklog

import (
    "os"
    "regexp"
)

func ExampleNewSubsystem() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    subCtx := NewSubsystem(exampleCtx, "my-subsystem")

    // messages logged to the subsystem will carry the subsystem name with
    // them
    SubsystemTrace(subCtx, "my-subsystem", "hello, world", map[string]interface{}{"foo": 123})

    // Output:
    // {"@level":"trace","@message":"hello, world","@module":"sdk.my-subsystem","foo":123}
}

func ExampleNewSubsystem_withLevel() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    os.Setenv("EXAMPLE_SUBSYSTEM_LEVEL", "WARN")

    // non-example-setup code begins here

    // create a context with a logger for a new "my-subsystem" subsystem,
    // using the WARN level from the "EXAMPLE_SUBSYSTEM_LEVEL" environment
    // variable
    subCtx := NewSubsystem(exampleCtx, "my-subsystem", WithLevelFromEnv("EXAMPLE_SUBSYSTEM_LEVEL"))

    // this won't actually get output, it's not at WARN or higher
    SubsystemTrace(subCtx, "my-subsystem", "hello, world", map[string]interface{}{"foo": 123})

    // the parent logger will still output at its configured TRACE level,
    // though
    Trace(subCtx, "hello, world", map[string]interface{}{"foo": 123})

    // and the subsystem logger will output at the WARN level
    SubsystemWarn(subCtx, "my-subsystem", "hello, world", map[string]interface{}{"foo": 123})

    // Output:
    // {"@level":"trace","@message":"hello, world","@module":"sdk","foo":123}
    // {"@level":"warn","@message":"hello, world","@module":"sdk.my-subsystem","foo":123}
}

func ExampleSubsystemSetField() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    derivedCtx := SubsystemSetField(exampleCtx, "my-subsystem", "foo", 123)

    // all messages logged with derivedCtx will now have foo=123
    // automatically included
    SubsystemTrace(derivedCtx, "my-subsystem", "example log message")

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk.my-subsystem","foo":123}
}

func ExampleSubsystemTrace() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    SubsystemTrace(exampleCtx, "my-subsystem", "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"trace","@message":"hello, world","@module":"sdk.my-subsystem","colors":["red","blue","green"],"foo":123}
}

func ExampleSubsystemDebug() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    SubsystemDebug(exampleCtx, "my-subsystem", "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"debug","@message":"hello, world","@module":"sdk.my-subsystem","colors":["red","blue","green"],"foo":123}
}

func ExampleSubsystemInfo() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    SubsystemInfo(exampleCtx, "my-subsystem", "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"info","@message":"hello, world","@module":"sdk.my-subsystem","colors":["red","blue","green"],"foo":123}
}

func ExampleSubsystemWarn() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    SubsystemWarn(exampleCtx, "my-subsystem", "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"warn","@message":"hello, world","@module":"sdk.my-subsystem","colors":["red","blue","green"],"foo":123}
}

func ExampleSubsystemError() {
    // this function calls new with the options it needs to be reliably
    // tested. framework and sdk developers should call new, inject the
    // resulting context in their framework, and then pass it around. this
    // examplectx is a stand-in for a context you have injected a logger
    // into and passed to the area of the codebase you need it.
    exampleCtx := getExampleContext()
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")

    // non-example-setup code begins here
    SubsystemError(exampleCtx, "my-subsystem", "hello, world", map[string]interface{}{
        "foo":    123,
        "colors": []string{"red", "blue", "green"},
    })

    // Output:
    // {"@level":"error","@message":"hello, world","@module":"sdk.my-subsystem","colors":["red","blue","green"],"foo":123}
}

func ExampleSubsystemMaskFieldValuesWithFieldKeys() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskFieldValuesWithFieldKeys(exampleCtx, "my-subsystem", "field1")

    // all messages logged with exampleCtx will now have field1=***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "field1": 123,
        "field2": 456,
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk.my-subsystem","field1":"***","field2":456}
}

func ExampleSubsystemMaskAllFieldValuesRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskAllFieldValuesRegexes(exampleCtx, "my-subsystem", regexp.MustCompile("v1|v2"))

    // all messages logged with exampleCtx will now have field values matching
    // the above regular expressions, replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk.my-subsystem","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleSubsystemMaskAllFieldValuesStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskAllFieldValuesStrings(exampleCtx, "my-subsystem", "v1", "v2")

    // all messages logged with exampleCtx will now have field values equal
    // the above strings, replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log message","@module":"sdk.my-subsystem","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleSubsystemMaskMessageStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskMessageStrings(exampleCtx, "my-subsystem", "my-sensitive-data")

    // all messages logged with exampleCtx will now have my-sensitive-data
    // replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message with my-sensitive-data masked")

    // Output:
    // {"@level":"trace","@message":"example log message with *** masked","@module":"sdk.my-subsystem"}
}

func ExampleSubsystemMaskMessageRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskMessageRegexes(exampleCtx, "my-subsystem", regexp.MustCompile(`[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}`))

    // all messages logged with exampleCtx will now have strings matching the
    // regular expression replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message with 1234-1234-1234-1234 masked")

    // Output:
    // {"@level":"trace","@message":"example log message with *** masked","@module":"sdk.my-subsystem"}
}

func ExampleSubsystemOmitLogWithFieldKeys() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemOmitLogWithFieldKeys(exampleCtx, "my-subsystem", "field1")

    // all messages logged with exampleCtx and using field1 will be omitted
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "field1": 123,
        "field2": 456,
    })

    // Output:
    //
}

func ExampleSubsystemOmitLogWithMessageStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemOmitLogWithMessageStrings(exampleCtx, "my-subsystem", "my-sensitive-data")

    // all messages logged with exampleCtx will now have my-sensitive-data
    // entries omitted
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message with my-sensitive-data masked")

    // Output:
    //
}

func ExampleSubsystemOmitLogWithMessageRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemOmitLogWithMessageRegexes(exampleCtx, "my-subsystem", regexp.MustCompile(`[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}`))

    // all messages logged with exampleCtx will be omitted if they have a
    // string matching the regular expression
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message with 1234-1234-1234-1234 masked")

    // Output:
    //
}

func ExampleSubsystemMaskLogRegexes() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskLogRegexes(exampleCtx, "my-subsystem", regexp.MustCompile("v1|v2"), regexp.MustCompile("message"))

    // all messages logged with exampleCtx will now have message content
    // and field values matching the above regular expressions, replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log ***","@module":"sdk.my-subsystem","k1":"*** plus some text","k2":"*** plus more text"}
}

func ExampleSubsystemMaskLogStrings() {
    // virtually no plugin developers will need to worry about
    // instantiating loggers, as the libraries they're using will take care
    // of that, but we're not using those libraries in these examples. So
    // we need to do the injection ourselves. Plugin developers will
    // basically never need to do this, so the next line can safely be
    // considered setup for the example and ignored. Instead, use the
    // context passed in by the framework or library you're using.
    exampleCtx := getExampleContext()

    // non-example-setup code begins here
    exampleCtx = NewSubsystem(exampleCtx, "my-subsystem")
    exampleCtx = SubsystemMaskLogStrings(exampleCtx, "my-subsystem", "v1", "v2", "message")

    // all messages logged with exampleCtx will now have message content
    // and field values equal the above strings, replaced with ***
    SubsystemTrace(exampleCtx, "my-subsystem", "example log message", map[string]interface{}{
        "k1": "v1 plus some text",
        "k2": "v2 plus more text",
    })

    // Output:
    // {"@level":"trace","@message":"example log ***","@module":"sdk.my-subsystem","k1":"*** plus some text","k2":"*** plus more text"}
}
