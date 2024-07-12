package expect

import (
    "testing"
    "time"
)

type ExpectNumericTests struct{}

func Test_ExpectNumeric(t *testing.T) {
    Expectify(new(ExpectNumericTests), t)
}

func (_ *ExpectNumericTests) Greater_Success() {
    eachNumeric(10, 9, func(a, b interface{}) {
        Expect(a).Greater.Than(b)
    })

    Expect(float32(10.1)).Greater.Than(float32(-10.0))
    Expect(float64(-3.33)).Greater.Than(float64(-4))
}

func (_ *ExpectNumericTests) Greater_Success_Time() {
    Expect(time.Now()).Greater.Than(time.Now().Add(time.Second * -2))
}

func (_ *ExpectNumericTests) Greater_Failures() {
    failing("expected 9 to be greater than 10", 9, func() {
        eachNumeric(9, 10, func(a, b interface{}) {
            Expect(a).Greater.Than(b)
        })
    })

    failing("expected 10 to be greater than 10", 9, func() {
        eachNumeric(10, 10, func(a, b interface{}) {
            Expect(a).Greater.Than(b)
        })
    })

    failing("expected -2.3 to be greater than -2.299", 2, func() {
        Expect(float32(-2.3)).Greater.Than(float32(-2.299))
        Expect(float64(-2.3)).Greater.Than(float64(-2.299))
    })

    failing("expected 33 to be greater than 1.2 - type mismatch int != float64", 1, func() {
        Expect(33).Greater.Than(1.2)
    })

    failing("cannot use greater than for type string", 1, func() {
        Expect("a").Greater.Than("b")
    })
}

func (_ *ExpectNumericTests) Greater_Failure_Time() {
    failing("expected 2014-12-07 04:18:10 +0000 UTC to be greater than 2014-12-07 04:18:11 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925890, 0).UTC()).Greater.Than(time.Unix(1417925891, 0).UTC())
    })

    failing("expected 2014-12-07 04:18:10 +0000 UTC to be greater than 2014-12-07 04:18:10 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925890, 0).UTC()).Greater.Than(time.Unix(1417925890, 0).UTC())
    })
}

func (_ *ExpectNumericTests) GreaterOrEqual_Success() {
    eachNumeric(10, 9, func(a, b interface{}) {
        Expect(a).GreaterOrEqual.To(b)
    })
    eachNumeric(10, 10, func(a, b interface{}) {
        Expect(a).GreaterOrEqual.To(b)
    })

    Expect(float32(10.1)).GreaterOrEqual.To(float32(-10.0))
    Expect(float32(10.1)).GreaterOrEqual.To(float32(10.1))
    Expect(float64(-3.33)).GreaterOrEqual.To(float64(-4))
    Expect(float64(-3.33)).GreaterOrEqual.To(float64(-3.33))
}

func (_ *ExpectNumericTests) GreaterOrEqual_Success_Time() {
    now := time.Now()
    Expect(now).GreaterOrEqual.To(now.Add(time.Second * -2))
    Expect(now).GreaterOrEqual.To(now)
}

func (_ *ExpectNumericTests) GreaterOrEqual_Failures() {
    failing("expected 9 to be greater or equal to 10", 9, func() {
        eachNumeric(9, 10, func(a, b interface{}) {
            Expect(a).GreaterOrEqual.To(b)
        })
    })

    failing("expected -2.3 to be greater or equal to -2.299", 2, func() {
        Expect(float32(-2.3)).GreaterOrEqual.To(float32(-2.299))
        Expect(float64(-2.3)).GreaterOrEqual.To(float64(-2.299))
    })
}

func (_ *ExpectNumericTests) GreaterOrEqual_Failure_Time() {
    failing("expected 2014-12-07 04:18:10 +0000 UTC to be greater or equal to 2014-12-07 04:18:11 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925890, 0).UTC()).GreaterOrEqual.To(time.Unix(1417925891, 0).UTC())
    })
}

func (_ *ExpectNumericTests) Not_Success() {
    Expect(492).Not.Greater.Than(493)
    Expect(493).Not.Greater.Than(493)

    Expect(492).Not.GreaterOrEqual.To(493)

    Expect(495).Not.Less.Than(494)
    Expect(495).Not.Less.Than(495)

    Expect(496).Not.LessOrEqual.To(495)
}

func (_ *ExpectNumericTests) Not_Failures() {
    failing("expected 494 not to be greater than 493", 1, func() {
        Expect(494).Not.Greater.Than(493)
    })

    failing("expected 495 not to be greater or equal to 494", 1, func() {
        Expect(495).Not.GreaterOrEqual.To(494)
    })

    failing("expected 495 not to be greater or equal to 495", 1, func() {
        Expect(495).Not.GreaterOrEqual.To(495)
    })

    failing("expected 22 not to be less than 23", 1, func() {
        Expect(22).Not.Less.Than(23)
    })

    failing("expected 11 not to be less or equal to 12", 1, func() {
        Expect(11).Not.LessOrEqual.To(12)
    })

    failing("expected 2 not to be less or equal to 2", 1, func() {
        Expect(2).Not.LessOrEqual.To(2)
    })
}

func (_ *ExpectNumericTests) Less_Success() {
    eachNumeric(9, 10, func(a, b interface{}) {
        Expect(a).Less.Than(b)
    })

    Expect(float32(-10.0)).Less.Than(float32(11.0))
    Expect(float64(-3.34)).Less.Than(float64(-3.33339))
}

func (_ *ExpectNumericTests) Less_Success_Time() {
    Expect(time.Now()).Less.Than(time.Now().Add(time.Second * 2))
}

func (_ *ExpectNumericTests) Less_Failures() {
    failing("expected 23 to be less than 22", 9, func() {
        eachNumeric(23, 22, func(a, b interface{}) {
            Expect(a).Less.Than(b)
        })
    })

    failing("expected 123 to be less than 123", 9, func() {
        eachNumeric(123, 123, func(a, b interface{}) {
            Expect(a).Less.Than(b)
        })
    })

    failing("expected -2.299 to be less than -2.3", 2, func() {
        Expect(float32(-2.299)).Less.Than(float32(-2.3))
        Expect(float64(-2.299)).Less.Than(float64(-2.3))
    })

    failing("expected 4.01 to be less than 4.01", 2, func() {
        Expect(float32(4.01)).Less.Than(float32(4.01))
        Expect(float64(4.01)).Less.Than(float64(4.01))
    })

    failing("expected 33 to be less than 1.2 - type mismatch int != float64", 1, func() {
        Expect(33).Less.Than(1.2)
    })

    failing("cannot use less than for type string", 1, func() {
        Expect("a").Less.Than("b")
    })
}

func (_ *ExpectNumericTests) Less_Failure_Time() {
    failing("expected 2014-12-07 04:18:11 +0000 UTC to be less than 2014-12-07 04:18:10 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925891, 0).UTC()).Less.Than(time.Unix(1417925890, 0).UTC())
    })

    failing("expected 2014-12-07 04:18:10 +0000 UTC to be less than 2014-12-07 04:18:10 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925890, 0).UTC()).Less.Than(time.Unix(1417925890, 0).UTC())
    })
}

func (_ *ExpectNumericTests) LessOrEqual_Success() {
    eachNumeric(9, 10, func(a, b interface{}) {
        Expect(a).LessOrEqual.To(b)
    })
    eachNumeric(10, 10, func(a, b interface{}) {
        Expect(a).LessOrEqual.To(b)
    })

    Expect(float32(3.2)).LessOrEqual.To(float32(44.1))
    Expect(float32(0.1)).LessOrEqual.To(float32(0.1))
    Expect(float64(-4.1)).LessOrEqual.To(float64(-4.0001))
    Expect(float64(-3.33)).LessOrEqual.To(float64(-3.33))
}

func (_ *ExpectNumericTests) LessOrEqual_Success_Time() {
    now := time.Now()
    Expect(now).LessOrEqual.To(now.Add(time.Second * 2))
    Expect(now).LessOrEqual.To(now)
}

func (_ *ExpectNumericTests) LessOrEqual_Failures() {
    failing("expected 3 to be less or equal to 1", 9, func() {
        eachNumeric(3, 1, func(a, b interface{}) {
            Expect(a).LessOrEqual.To(b)
        })
    })

    failing("expected -2.291 to be less or equal to -2.292", 2, func() {
        Expect(float32(-2.291)).LessOrEqual.To(float32(-2.292))
        Expect(float64(-2.291)).LessOrEqual.To(float64(-2.292))
    })
}

func (_ *ExpectNumericTests) LessOrEqual_Failure_Time() {
    failing("expected 2014-12-07 04:18:11 +0000 UTC to be less or equal to 2014-12-07 04:18:10 +0000 UTC", 1, func() {
        Expect(time.Unix(1417925891, 0).UTC()).LessOrEqual.To(time.Unix(1417925890, 0).UTC())
    })
}

func (_ *ExpectNumericTests) Equal_Success() {
    eachNumeric(101, 101, func(a, b interface{}) {
        Expect(a).To.Equal(b)
    })

    Expect(float32(0.12)).To.Equal(float32(0.12))
    Expect(float64(-3.33011)).To.Equal(float64(-3.33011))
}

func (_ *ExpectNumericTests) Equal_Failures() {
    failing("expected 3 to be equal to 1", 9, func() {
        eachNumeric(3, 1, func(a, b interface{}) {
            Expect(a).To.Equal(b)
        })
    })

    failing("expected -2.291 to be equal to -2.292", 2, func() {
        Expect(float32(-2.291)).To.Equal(float32(-2.292))
        Expect(float64(-2.291)).To.Equal(float64(-2.292))
    })
}

func (_ *ExpectNumericTests) NotEqual_Success() {
    eachNumeric(101, 100, func(a, b interface{}) {
        Expect(a).Not.To.Equal(b)
    })

    Expect(float32(0.12)).Not.To.Equal(float32(0.112))
    Expect(float64(-3.33011)).Not.To.Equal(float64(-3.330111))
}

func (_ *ExpectNumericTests) NotEqual_Failures() {
    failing("expected 33 not to equal 33", 9, func() {
        eachNumeric(33, 33, func(a, b interface{}) {
            Expect(a).Not.To.Equal(b)
        })
    })

    failing("expected -2.291 not to equal -2.291", 2, func() {
        Expect(float32(-2.291)).Not.To.Equal(float32(-2.291))
        Expect(float64(-2.291)).Not.To.Equal(float64(-2.291))
    })
}

func eachNumeric(a, b int, f func(ai, bi interface{})) {
    f(a, b)
    f(int8(a), int8(b))
    f(int16(a), int16(b))
    f(int32(a), int32(b))
    f(int64(a), int64(b))
    f(uint8(a), uint8(b))
    f(uint16(a), uint16(b))
    f(uint32(a), uint32(b))
    f(uint64(a), uint64(b))
}
