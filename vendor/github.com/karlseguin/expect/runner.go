package expect

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wsxiaoys/terminal/color"
)

var (
	loaded         = false
	loadLock       = new(sync.Mutex)
	showStdout     = flag.Bool("vv", false, "turn on stdout")
	exitOnFailure  = flag.Bool("e", false, "exit on failures")
	matchFlag      = flag.String("m", "", "Regular expression selecting which tests to run")
	notMatchFlag   = flag.String("M", "", "Regular expression selecting which tests not to run")
	summaryPath    = flag.String("summary", "", "Path to write a summary file to")
	pattern        *regexp.Regexp
	notPattern     *regexp.Regexp
	runner         *Runner
	stdout         = os.Stdout
	silentOut      *os.File
	beforeEach     = make([]func(), 0, 2)
	endTestErr     = new(error)
	summaryPattern = regexp.MustCompile(`(?s)(\d+) passed.*?(\d+) failed`)
)

func init() {
	os.Setenv("GO_TEST", "true")
}

func Expectify(suite interface{}, t *testing.T) {
	loadLock.Lock()
	if !loaded {
		flag.Parse()
		if len(*matchFlag) != 0 {
			pattern = regexp.MustCompile("(?i)" + *matchFlag)
		}
		if len(*notMatchFlag) != 0 {
			notPattern = regexp.MustCompile("(?i)" + *notMatchFlag)
		}
		if *showStdout == true {
			silentOut = stdout
		}
		os.Stdout = silentOut
		loaded = true

		if *exitOnFailure {
			FailureHandlerFactory = func(actual, expected interface{}) PostHandler {
				for _, res := range runner.results {
					if !res.Passed() || testing.Verbose() {
						res.Report()
					}
				}
				os.Exit(1)
				return nil
			}
		}
	}
	loadLock.Unlock()

	var name string
	var res *result
	defer func() {
		if err := recover(); err != nil {
			if err == endTestErr {
				finish(t)
				return
			}
			os.Stdout = stdout
			if res != nil {
				res.Report()
			}
			color.Printf("@R 💣  %-75s\n", name)
			panic(err)
		}
	}()

	tp := reflect.TypeOf(suite)
	sv := reflect.ValueOf(suite)
	count := tp.NumMethod()

	runner = &Runner{
		results: make([]*result, 0, 10),
	}

	each, _ := tp.MethodByName("Each")
	if each.Func.IsValid() && each.Type.NumIn() != 2 {
		each = reflect.Method{}
	}

	announced := false
	for i := 0; i < count; i++ {
		method := tp.Method(i)
		// this method is not exported
		if len(method.PkgPath) != 0 {
			continue
		}
		name = method.Name
		typeName := sv.Elem().Type().String()

		if method.Type.NumIn() != 1 {
			continue
		}

		if pattern != nil && !pattern.MatchString(name) && !pattern.MatchString(typeName) {
			continue
		}
		if notPattern != nil && (notPattern.MatchString(name) || notPattern.MatchString(typeName)) {
			continue
		}

		os.Stdout = stdout
		res = runner.Start(name, typeName)
		var f = func() {
			method.Func.Call([]reflect.Value{sv})
			if runner.End() == false || testing.Verbose() {
				if announced == false {
					color.Printf("\n@!%s@|\n", typeName)
					announced = true
				}
				res.Report()
			}
		}
		for i := 0; i < len(beforeEach); i++ {
			beforeEach[i]()
		}
		if each.Func.IsValid() {
			each.Func.Call([]reflect.Value{sv, reflect.ValueOf(f)})
		} else {
			f()
		}
		os.Stdout = silentOut
	}
	finish(t)
}

func finish(t *testing.T) {
	passed := 0
	for _, result := range runner.results {
		if result.Passed() {
			passed++
		}
	}
	failed := len(runner.results) - passed
	if failed != 0 {
		os.Stdout = stdout
		fmt.Println("\nFailure summary")
		for _, r := range runner.results {
			if r.Passed() == false {
				r.Summary()
			}
		}
		fmt.Println()
		os.Stdout = silentOut
		t.Fail()
	}
	if path := *summaryPath; len(path) != 0 {
		updatePersistedSummary(path, passed)
	}
}

func updatePersistedSummary(path string, passed int) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	file.Write([]byte(strconv.Itoa(passed)))
	file.Write([]byte{'\n'})
	file.Close()
}

func BeforeEach(f func()) {
	beforeEach = append(beforeEach, f)
}

type Runner struct {
	results []*result
	current *result
}

func (r *Runner) Start(name string, typeName string) *result {
	r.current = &result{
		method:   name,
		typeName: typeName,
		start:    time.Now(),
		failures: make([]*Failure, 0, 3),
	}
	r.results = append(r.results, r.current)
	return r.current
}

func (r *Runner) End() bool {
	r.current.end = time.Now()
	passed := r.current.Passed()
	r.current = nil
	return passed
}

func (r *Runner) Skip(format string, args ...interface{}) {
	if r.current != nil {
		r.current.Skip(format, args...)
	}
}

func (r *Runner) Errorf(format string, args ...interface{}) {
	collecting := false
	stacks := make([]string, 0, 5)

	for i := 0; i < 20; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok == false {
			break
		}

		isTest := strings.HasSuffix(file, "_test.go")
		if collecting == false && isTest {
			collecting = true
		}

		// we're no longer in the _test.go stack, stop collecting
		if collecting == true && !isTest {
			break
		}
		if collecting {
			stacks = append(stacks, fmt.Sprintf("       %s:%d", file, line))
		}
	}

	failure := &Failure{
		message:  fmt.Sprintf(format, args...),
		location: strings.Join(stacks, "\n") + "\n",
	}
	r.current.failures = append(r.current.failures, failure)
}

func (r *Runner) ErrorMessage(format string, args ...interface{}) {
	if r.current != nil {
		r.current.ErrorMessage(format, args...)
	}
}

type result struct {
	method      string
	failures    []*Failure
	typeName    string
	start       time.Time
	end         time.Time
	skipMessage string
	skip        bool
}

type Failure struct {
	message  string
	location string
}

func (r *result) Skip(format string, args ...interface{}) {
	r.skip = true
	r.skipMessage = fmt.Sprintf(format, args...)
}

func (r *result) Passed() bool {
	return r.skip || len(r.failures) == 0
}

func (r *result) ErrorMessage(format string, args ...interface{}) {
	l := len(r.failures)
	if l > 0 {
		r.failures[l-1].message = fmt.Sprintf(format, args...)
	}
}

func (r *result) Report() {
	if r.end.IsZero() {
		r.end = time.Now()
	}
	info := fmt.Sprintf(" %-70s%dms", r.method, r.end.Sub(r.start).Nanoseconds()/1000000)
	if r.skip {
		color.Println(" @y⸚", info)
		color.Println("   @." + r.skipMessage)
	} else if r.Passed() {
		color.Println(" @g✓", info)
	} else {
		color.Println(" @r×", info)
		for i, failure := range r.failures {
			color.Printf("    %d. %s\n@.%-40s\n", i+1, failure.message, failure.location)
		}
	}
}

func (r *result) Summary() {
	info := fmt.Sprintf(" %s.%-40s", r.typeName, r.method)
	if r.skip {
		color.Println(" @y⸚", info)
	} else if r.Passed() {
		color.Println(" @g✓", info)
	} else {
		color.Print(" @r×", info)
		color.Printf("%2d\n", len(r.failures))
	}
}
