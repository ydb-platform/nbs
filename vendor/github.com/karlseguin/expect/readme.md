# Expect

A library to help you write tests in Go.

What's wrong with Go's built-in testing package? Not much, except it tends to lead to verbose code. `Expect` runs within `go test` but provides a different syntax for specifying expectations.

## Example

```go
import (
  "testing"
  . "github.com/karlseguin/expect"
)

type CalculatorTests struct{}

func Test_Caculator(t *testing.T) {
  Expectify(new(CalculatorTests), t)
}

func (_ CalculatorTests) AddsTwoNumbers() {
  c := new(Calculator)
  Expect(c.Add(4, 8)).To.Equal(12)
  Expect(c.Add(10, 2)).Greater.Than(11)
  Expect(c.Add(10, 2)).LessOrEqual.To(12)
  Expect(c.Add(10, 2)).Not.Greater.Than(9000)
  Expect(c.Add(1, 1)).Not.To.Equal(3)

  //OR

  Expect(c.Add(4.8)).ToEqual(12)
  Expect(c.Add(10, 2)).GreaterThan(11)
  Expect(c.Add(10, 2)).LessOrEqualTo(12)
  Expect(c.Add(10, 2)).Not.GreaterThan(9000)
  Expect(c.Add(1, 1)).Not.ToEqual(3)
}
```

You can also use `Skip(format, args...)` and `Fail(format, args...)` to either skip a test or cause a test to fail.

## Running

Run tests as you normally would via `go test`. However, to run specific tests, use the -m flag, which will do a case-insensitive regular expression match.

    go test -m AddsTwo

### Exit On Error
Use the -e flag to exit on a failed assertion.

## Expectations

Two similar syntaxes of expectations are supported

* `To.Equal(x)` or `ToEqual(x)`
* `Greater.Than(x)` or `GreaterThan(x)`
* `GreaterOrEqual.To(x)` or `GreaterOrEqualTo(x)`
* `Less.Than(x)` or `LessThan(x)`
* `LessOrEqual.To(x)` or `LessOrEqualTo(x)`

All expectations can be reversed by starting the chain with `Not.`:

* `Not.To.Equal(x)` or `Not.ToEqual(x)`
* `Not.Greater.Than(x)` or `Not.GreaterThan(x)`
* `Not.GreaterOrEqual.To(x)` or `Not.GreaterOrEqualTo(x)`
* `Not.Less.Than(x)` or `Not.LessThan(x)`
* `Not.LessOrEqual.To(x)` or `Not.LessOrEqualTo(x)`
verbose
### Eql

The `Equal` method is strict. This will fail:

```go
  //fails
  Expect(uint32(449)).To.Equal(449)
```

`Eql` can be used to do implicit type conversion for common types (notably between the various integer types, as well as []byte <-> string).

```go
  //passes
  Expect(uint32(449)).To.Eql(449)
  Expect([]byte{65, 66}).To.Eql("AB")
```

### JSON

`Expect` exposes a `JSON` helper:

```go
Expect(`{"spice":"must flow"}`).To.Equal(JSON(`
{
  "spice": "must flow"
}`))
```

The helper makes it possible to write a expectations which ignore white spaces and are thus easier to write, read and maintain. Internally, when the `JSON` helper is used, both values (actual and expected) are unmarshalled using the encoding/json library (essentially converted to an map[string]interface{}) and a then compared in this form.

### Contains

`To.Contain` works with strings, arrays, slices and maps. For arrays and slices, only individual values are matched. For example:

```go
Expect([]int{1,2,3}).To.Contain([]int{1,2})
```

will, sadly, not work.

The exception to this is for strings and `[]byte`. These work with either a single value or an array (they use the stdlib's `strings.Contains` and `bytes.Contains`).

## Additional Information

You can add extra information to an error by using the `Message(format string, args ...interface{})` function on an expectation:

```go
Expect(res.Code).To.Equal(404).Message("path: %s", path)
```

This can be useful when you're testing the same code against different inputs (probably in a `for` loop).

Given a message with a layout but not arguments, `Message` will inject the actual
and expected values.

```go
Expect(res.Code).To.Equal(404).Message("Expected %d got %d")
```

## Multiple Values

`Expect` will only compare the number of values expected. This is useful for ignoring nil errors:

```go
Expect(ioutil.ReadFile("blah")).To.Equal([]byte{1, 2, 3, 4})
```

Multiple expected values can be provided and they'll be compared to the corresponding value:

```go
Expect(1, true, "a").To.Equal(1, true)
Expect(1, true, "a").To.Equal(1, true, "a")

Expect(1, true, "a").To.Equal(1, true, "a", "NOPE") // will fail
```

## stdout

Go's testing package has no hooks into its reporting. `Expect` takes the drastic step of occasionally silencing `os.Stdout`, which many packages use (such as `fmt.Print`). However, within your test, `os.Stdout` **will** work.

If you print anything outside of your test, say during `init`, it'll likely be silenced by `Expect`. You can disable this behavior with the `-vv` flag (use `-v` and `-vv` in combination to change the behavior of both `Expect` and Go's library)

## Mixing with *testing.T

Since `Expect` runs within `go test`, you can mix `Expect` style tests with traditional Go tests. To do this, you'll probably want to run your tests with `-vv` (see *stdout* section above). Even without `-vv`, you *will* get the proper return code (!= 0 on failure).

This also means that `go test` features, such as `-cover`, work with `Expect`. However, `Expect` tests cannot be run in parallel.

## Advanced Patterns

Since tests are organized within a class, there are patterns you can used for tests which require special care. For example, unexported methods can be used for helpers without conflicting with other helpers:

```go
func (ct *CalculatorTests) AddsTwoNumbers() {
  c := ct.n()
  // ...
}

func (ct *CalculatorTests) n() *Calculator {
  return &Calculator{...}
}
```

Furthermore, when you setup the runner, you can setup code to before or after running tests (once for all tests, not once per test):

```go
func Test_Caculator(t *testing.T) {
  // before all tests
  Expectify(new(CalculatorTests), t)
  // after all tests
}
```

### Before / After Each

You can intercept each run test by creating a method named `Each` which takes a single parameter of type `func()`:

```go
func (_ *CalculatorTests) Each(f func()) {
  //run code before each test
  f()
  //run code after each test
}
```


### Global Before Each
A before each can be registered for all suites. This function will be called before any suite-specific Each.

```go
func init() {
  BeforeEach(func() {
    //do something
  })
}
```

## Summary File
If you have many tests, you may find it cumbersome to run with -v and/or -vv, yet still want
a brief summary beyond what Go's test runner provides (I always worry that everything passed because nothing ran). However, because of the way that Go's running takes over stdout and the fact that it potentially launches multiple processes, writing a final summary is not something we can do as easily as we'd like.

What we can do is use the `-summary <PATH>` flag and provide a path where summary data will be stored / updated. Therefore, even if multiple processes are launched, and even if we can't output to stdout, with a bit of work invoking the tests, we can get a summary. For example, I recommend a simple Makefile:

```Makefile
TMPDIR := $(shell dirname $(shell mktemp -u))
TEST_SUMMARY := $(TMPDIR)/$$(go list).go.summary

test:
  go test ./... -summary $(TEST_SUMMARY)
  @if [ -a $(TEST_SUMMARY) ] ; \
  then \
    awk '{s+=$$1} END {print "\033[1;32m", s, "passed"}' $(TEST_SUMMARY); rm $(TEST_SUMMARY) ;\
  fi;
```

# Mocks

The `mock` sub-package provides mock objects and buiders for common standard library components.

**Work in progress**

## net.Conn

The `MockConn` lets you test both the reading and writing to a `net.Conn` object.

### Writes
Writes sent to the mock object are captured in the `Written` field. Note that this is a `[][]byte` - it captures each write individually.

### Reads
You can `Read` from the mock object by first priming it with data via the `Reading` function:

```go
conn := mock.Conn().Reading([]byte("hello"), []byte("world"))
```

To drain this connection `Read` will have to be called twice provided a large enough buffer is provided. If the supplied buffer is too small, the mock object will behave as expected, reading what it can, returning the length read and properly handling subsequent requests to `Read`.

Rather than reading data, the mock object can be primed to return an error:

```go
conn := mock.Conn().Error(errors.New("some error"))
```

This will cause the first call to `Read` to return an error

# Builders

The `build` sub-package provides build helpers for common standard library components. Builders expose a fluent interface for setting various properties. The default object returned from a builder should be safe to use as-is.

**Work in progress**

## *http.Request

```go
req := build.Request().
        Method("GET").
        URLString("http://openmymind.net/test").
        Request
```

* `Method(method string)`
* `Proto(major, minor int)`
* `URL(u *url.URL)`
* `URLString(url string)` - panics if `url` isn't a valid URL
* `Path(path string)` - changes the `path` component of the URL only
* `RawQuery(query string)` - changes the `rawquery` component of the URL only
* `Host(host string)` - changes the `host` component of the URL only
* `Header(key, value string)` - set the specified header
* `Body(body string)` - set's the request's body
