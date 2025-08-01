package fixenv

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

var errTooManyOptionalArgs = errors.New("allow not more then one optional arg")

// FatalfFunction function signature of Fatalf
type FatalfFunction func(format string, args ...interface{})

// SkipNowFunction is function signature for SkipNow
type SkipNowFunction func()

// CreateMainTestEnvOpts is options for manage package env scope
type CreateMainTestEnvOpts struct {
	// Fatalf equivalent of Fatalf in test.
	// Must write log, then exit from goroutine.
	// It may be panic.
	// Fatalf called if main envinment can't continue work
	Fatalf FatalfFunction

	// SkipNow is equivalent of SkipNow in test
	// default is panic
	//
	// SkipNow marks the test as having been skipped and stops its execution
	// by calling runtime.Goexit.
	// If a test fails (see Error, Errorf, Fail) and is then skipped,
	// it is still considered to have failed.
	// Execution will continue at the next test or benchmark. See also FailNow.
	// SkipNow must be called from the goroutine running the test, not from
	// other goroutines created during the test. Calling SkipNow does not stop
	// those other goroutines.
	SkipNow SkipNowFunction
}

// packageLevelVirtualTest now used for tests only
var lastPackageLevelVirtualTest *virtualTest

// CreateMainTestEnv called from TestMain for create global environment.
// It need only for use ScopePackage cache scope.
// If ScopePackage not used - no need to create main env.
func CreateMainTestEnv(opts *CreateMainTestEnvOpts) (env *EnvT, tearDown func()) {
	// TODO: handle second time initialize
	globalMutex.Lock()
	packageLevelVirtualTest := newVirtualTest(opts)
	lastPackageLevelVirtualTest = packageLevelVirtualTest
	globalMutex.Unlock()

	env = New(packageLevelVirtualTest) // register global test for env
	return env, packageLevelVirtualTest.cleanup
}

// RunTests runs the tests. It returns an exit code to pass to os.Exit.
//
// Usage:
// declare in _test file TestMain function:
//
// func TestMain(m *testing.M) {
//	 os.Exit(fixenv.RunTests(m))
// }
func RunTests(m RunTestsI, opts ...CreateMainTestEnvOpts) int {
	var options *CreateMainTestEnvOpts
	switch len(opts) {
	case 0:
		// pass
	case 1:
		options = &opts[0]
	default:
		panic(errTooManyOptionalArgs)
	}

	_, cancel := CreateMainTestEnv(options)
	defer cancel()
	return m.Run()
}

type RunTestsI interface {
	// Run runs the tests. It returns an exit code to pass to os.Exit.
	Run() (code int)
}

// virtualTest implement T interface for global env scope
type virtualTest struct {
	m       sync.Mutex
	fatalf  FatalfFunction
	skipNow SkipNowFunction

	cleanups []func()
	skipped  bool
}

func newVirtualTest(opts *CreateMainTestEnvOpts) *virtualTest {
	if opts == nil {
		opts = &CreateMainTestEnvOpts{}
	}
	t := &virtualTest{
		fatalf:  opts.Fatalf,
		skipNow: opts.SkipNow,
	}

	if t.fatalf == nil {
		t.fatalf = func(format string, args ...interface{}) {
			panic(fmt.Sprintf(format, args...))
		}
	}

	if t.skipNow == nil {
		t.skipNow = func() {
			panic("fixenv: skip called for TestMain without define skip function")
		}
	}

	return t
}

func (t *virtualTest) Cleanup(f func()) {
	t.m.Lock()
	defer t.m.Unlock()

	t.cleanups = append(t.cleanups, f)
}

func (t *virtualTest) Fatalf(format string, args ...interface{}) {
	t.fatalf(format, args...)
}

func (t *virtualTest) Logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (t *virtualTest) Name() string {
	return packageScopeName
}

func (t *virtualTest) SkipNow() {
	t.m.Lock()
	t.skipped = true
	t.m.Unlock()

	t.skipNow()
}

func (t *virtualTest) Skipped() bool {
	t.m.Lock()
	defer t.m.Unlock()

	return t.skipped
}

func (t *virtualTest) cleanup() {
	for i := len(t.cleanups) - 1; i >= 0; i-- {
		t.cleanups[i]()
	}
}
