package expect

import (
    "errors"
    "fmt"
    "testing"
)

var originalErrorf = Errorf
var beforeEachCalled = 0

type ExpectTests struct{}

func init() {
    BeforeEach(func() {
        beforeEachCalled++
    })
}

func Test_Expect(t *testing.T) {
    Expectify(new(ExpectTests), t)
}

func (_ ExpectTests) Equal_Success() {
    Expect("abc").To.Equal("abc")
    Expect(true).To.Equal(true)
    Expect(false).To.Equal(false)
    Expect([]int{1, 2, 3}).To.Equal([]int{1, 2, 3})
    Expect([]bool{true, false, false}).To.Equal([]bool{true, false, false})
    Expect(map[string]int{"a": 1, "b": 2}).To.Equal(map[string]int{"a": 1, "b": 2})
}

func (_ ExpectTests) Equal_Failures() {
    failing(`expected "abc" to be equal to "123"`, 1, func() {
        Expect("abc").To.Equal("123")
    })

    failing("expected true to be equal to false", 1, func() {
        Expect(true).To.Equal(false)
    })

    failing("expected [1 2] to be equal to [1]", 1, func() {
        Expect([]int{1, 2}).To.Equal([]int{1})
    })

    // hard to test, the print out is indeterministic, maps aren't sorted
    // failing("expected map[a:1 b:2] to be equal to map[b:1 a:2]", 1, func(){
    //     Expect(map[string]int{"a":1,"b":2}).Equal.To(map[string]int{"b":1,"a":2})
    // })
}

func (_ ExpectTests) NotEqual_Success() {
    Expect("abc").Not.To.Equal("123")
}

func (_ ExpectTests) NoEqual_Failures() {
    failing(`expected "123" not to equal "123"`, 1, func() {
        Expect("123").Not.To.Equal("123")
    })

    failing("expected true not to equal true", 1, func() {
        Expect(true).Not.To.Equal(true)
    })

    failing("expected [1 2] not to equal [1 2]", 1, func() {
        Expect([]int{1, 2}).Not.To.Equal([]int{1, 2})
    })
}

func (_ ExpectTests) Contain_Success() {
    Expect("abc").To.Contain("bc")
    Expect("abc").Not.To.Contain("99")

    Expect([]int{1, 3, 4}).To.Contain(4)
    Expect([]int{2, 3, 4}).Not.To.Contain(5)

    Expect(map[string]int{"a": 3}).To.Contain("a")
    Expect(map[string]int{"a": 3}).Not.To.Contain("b")

    Expect([]byte{1, 2, 3}).To.Contain(byte(1))
    Expect([]byte{1, 2, 3}).Not.To.Contain(byte(5))
    Expect([]byte{1, 2, 3}).Not.To.Contain([]byte{2, 3, 4})
}

func (_ ExpectTests) Contain_Failures() {
    failing("abc does not contain zz", 1, func() {
        Expect("abc").To.Contain("zz")
    })
    failing("aazzbb contains zz", 1, func() {
        Expect("aazzbb").Not.To.Contain("zz")
    })

    failing("[1 2] does not contain 3", 1, func() {
        Expect([]int{1, 2}).To.Contain(3)
    })
    failing("[1 2] contains 2", 1, func() {
        Expect([]int{1, 2}).Not.To.Contain(2)
    })

    failing("map[a:3] does not contain b", 1, func() {
        Expect(map[string]int{"a": 3}).To.Contain("b")
    })
    failing("map[a:3] contains a", 1, func() {
        Expect(map[string]int{"a": 3}).Not.To.Contain("a")
    })

    failing("[1 2 3] does not contain [3 4]", 1, func() {
        Expect([]byte{1, 2, 3}).To.Contain([]byte{3, 4})
    })
    failing("[1 2 3] contains [2 3]", 1, func() {
        Expect([]byte{1, 2, 3}).Not.To.Contain([]byte{2, 3})
    })
}

func (_ ExpectTests) ExpectMulipleValues_Success() {
    Expect(1, true, "over 9000").To.Equal(1, true, "over 9000")
    Expect([]byte("123"), nil).To.Equal([]byte("123"))
    Expect([]byte("123"), nil).To.Eql("123")
}

func (_ ExpectTests) ExpectMulipleValues_Failure() {
    failing("mismatch number of values and expectations 2 != 3", 1, func() {
        Expect(1, true).To.Equal(1, true, "a")
    })
    failing("expected 1 to be equal to 2", 1, func() {
        Expect(1, "a").To.Equal(2, "a")
    })
    failing(`expected "a" to be equal to "b"`, 1, func() {
        Expect(1, "a").To.Equal(1, "b")
    })
}

func (_ ExpectTests) ExpectNil_Success() {
    var n *string
    Expect(nil).To.Equal(nil)
    Expect(n).To.Equal(nil)
    Expect(n).To.Equal(n)
    Expect(nil).Not.To.Equal(32)
    Expect(32).Not.To.Equal(n)
}

func (_ ExpectTests) ExpectNil_Failure() {
    var n *string
    failing("expected <nil> to be equal to 44", 2, func() {
        Expect(nil).To.Equal(44)
        Expect(n).To.Equal(44)
    })
    failing("expected <nil> not to equal <nil>", 3, func() {
        Expect(nil).Not.To.Equal(nil)
        Expect(nil).Not.To.Equal(n)
        Expect(n).Not.To.Equal(n)
    })
}

func (_ ExpectTests) Fail() {
    failing("the failure reason 123", 1, func() {
        Fail("the failure reason %d", 123)
    })
}

func (_ ExpectTests) Skip_After() {
    Fail("failed")
    Skip("the skip reason %d", 9001)
}

func (_ ExpectTests) Skip_Before() {
    Skip("the skip reason")
    Fail("failed")
}

func (_ ExpectTests) Skip_NoPanic() {
    Skip("the skip reason")
    panic("should be swallowed")
}

func (_ ExpectTests) AltenrativeSyntax() {
    Expect(true).ToEqual(true)
    Expect(123).Not.ToEqual(11)
    Expect("it's over", 9000).ToEqual("it's over", 9000)
    Expect("it's not over", 90000).Not.ToEqual("it's over", 9000)

    Expect(94).GreaterThan(93)
    Expect(94).Not.GreaterThan(94)
    Expect(94).Not.GreaterThan(95)
    Expect(94).GreaterOrEqualTo(93)
    Expect(94).GreaterOrEqualTo(94)
    Expect(94).Not.GreaterOrEqualTo(95)

    Expect(12).LessThan(13)
    Expect(12).Not.LessThan(12)
    Expect(12).Not.LessThan(-1)
    Expect(12).LessOrEqualTo(12)
    Expect(12).LessOrEqualTo(12)
    Expect(12).Not.LessOrEqualTo(1)
}

func (_ ExpectTests) Json() {
    Expect(`{"about":true}`).To.Equal(JSON(`{
        "about": true
    }`))
}

func (_ ExpectTests) GlobalBeforEEach() {
    Expect(beforeEachCalled).Greater.Than(0)
}

func (_ ExpectTests) EqlForInts() {
    Expect(uint16(1)).To.Eql(1)
    Expect(uint32(2)).To.Eql(2)
    Expect(uint64(3)).To.Eql(3)
    Expect(int16(4)).To.Eql(4)
    Expect(int32(5)).To.Eql(5)
    Expect(int64(6)).To.Eql(6)
    Expect(byte(7)).To.Eql(7)
}

func (_ ExpectTests) EqlForStringAndBytes() {
    Expect([]byte{65, 66}).To.Eql("AB")
    Expect("CD").To.Eql([]byte{67, 68})
}

func (_ ExpectTests) EqlForStringAndError() {
    Expect(errors.New("an error")).To.Eql("an error")

    failing("expected \"an error\" to be equal to \"123\"", 1, func() {
        Expect(errors.New("an error")).To.Eql("123")
    })

    failing("expected <nil> to be equal to 123", 1, func() {
        Expect(nil).To.Eql("123")
    })
}

func failing(expected string, count int, f func()) {
    actuals := make([]string, 0, 5)
    Errorf = func(format string, args ...interface{}) {
        actuals = append(actuals, fmt.Sprintf(format, args...))
    }
    f()
    Errorf = originalErrorf
    if len(actuals) != count {
        Errorf("expected %d failures got %d", count, len(actuals))
        return
    }
    for i := 0; i < count; i++ {
        if actuals[i] != expected {
            if count == 1 {
                Errorf("expected failure '%s' got '%s'", expected, actuals[i])
            } else {
                Errorf("expected failure '%s' got '%s' index %d", expected, actuals[i], i+1)
            }
        }
    }
}
