// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ext

import (
    "fmt"
    "math"
    "reflect"
    "strings"
    "testing"
    "time"
    "unicode/utf8"

    "google.golang.org/protobuf/proto"

    "github.com/google/cel-go/cel"
    "github.com/google/cel-go/checker"
    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/common/types/ref"

    proto3pb "github.com/google/cel-go/test/proto3pb"
)

// TODO: move these tests to a conformance test.
var stringTests = []struct {
    expr      string
    err       string
    parseOnly bool
}{
    // CharAt test.
    {expr: `'tacocat'.charAt(3) == 'o'`},
    {expr: `'tacocat'.charAt(7) == ''`},
    {expr: `'©αT'.charAt(0) == '©' && '©αT'.charAt(1) == 'α' && '©αT'.charAt(2) == 'T'`},
    // Index of search string tests.
    {expr: `'tacocat'.indexOf('') == 0`},
    {expr: `'tacocat'.indexOf('ac') == 1`},
    {expr: `'tacocat'.indexOf('none') == -1`},
    {expr: `'tacocat'.indexOf('', 3) == 3`},
    {expr: `'tacocat'.indexOf('a', 3) == 5`},
    {expr: `'tacocat'.indexOf('at', 3) == 5`},
    {expr: `'ta©o©αT'.indexOf('©') == 2`},
    {expr: `'ta©o©αT'.indexOf('©', 3) == 4`},
    {expr: `'ta©o©αT'.indexOf('©αT', 3) == 4`},
    {expr: `'ta©o©αT'.indexOf('©α', 5) == -1`},
    {expr: `'ijk'.indexOf('k') == 2`},
    {expr: `'hello wello'.indexOf('hello wello') == 0`},
    {expr: `'hello wello'.indexOf('ello', 6) == 7`},
    {expr: `'hello wello'.indexOf('elbo room!!') == -1`},
    {expr: `'hello wello'.indexOf('elbo room!!!') == -1`},
    {expr: `'tacocat'.lastIndexOf('') == 7`},
    {expr: `'tacocat'.lastIndexOf('at') == 5`},
    {expr: `'tacocat'.lastIndexOf('none') == -1`},
    {expr: `'tacocat'.lastIndexOf('', 3) == 3`},
    {expr: `'tacocat'.lastIndexOf('a', 3) == 1`},
    {expr: `'ta©o©αT'.lastIndexOf('©') == 4`},
    {expr: `'ta©o©αT'.lastIndexOf('©', 3) == 2`},
    {expr: `'ta©o©αT'.lastIndexOf('©α', 4) == 4`},
    {expr: `'hello wello'.lastIndexOf('ello', 6) == 1`},
    {expr: `'hello wello'.lastIndexOf('low') == -1`},
    {expr: `'hello wello'.lastIndexOf('elbo room!!') == -1`},
    {expr: `'hello wello'.lastIndexOf('elbo room!!!') == -1`},
    {expr: `'hello wello'.lastIndexOf('hello wello') == 0`},
    {expr: `'bananananana'.lastIndexOf('nana', 7) == 6`},
    // Lower ASCII tests.
    {expr: `'TacoCat'.lowerAscii() == 'tacocat'`},
    {expr: `'TacoCÆt'.lowerAscii() == 'tacocÆt'`},
    {expr: `'TacoCÆt Xii'.lowerAscii() == 'tacocÆt xii'`},
    // Replace tests
    {expr: `"12 days 12 hours".replace("{0}", "2") == "12 days 12 hours"`},
    {expr: `"{0} days {0} hours".replace("{0}", "2") == "2 days 2 hours"`},
    {expr: `"{0} days {0} hours".replace("{0}", "2", 1).replace("{0}", "23") == "2 days 23 hours"`},
    {expr: `"1 ©αT taco".replace("αT", "o©α") == "1 ©o©α taco"`},
    {expr: `"hello hello".replace("", "_") == "_h_e_l_l_o_ _h_e_l_l_o_"`},
    {expr: `"hello hello".replace("h", "") == "ello ello"`},
    // Split tests.
    {expr: `"hello world".split(" ") == ["hello", "world"]`},
    {expr: `"hello world events!".split(" ", 0) == []`},
    {expr: `"hello world events!".split(" ", 1) == ["hello world events!"]`},
    {expr: `"o©o©o©o".split("©", -1) == ["o", "o", "o", "o"]`},
    // Substring tests.
    {expr: `"tacocat".substring(4) == "cat"`},
    {expr: `"tacocat".substring(7) == ""`},
    {expr: `"tacocat".substring(0, 4) == "taco"`},
    {expr: `"tacocat".substring(4, 4) == ""`},
    {expr: `'ta©o©αT'.substring(2, 6) == "©o©α"`},
    {expr: `'ta©o©αT'.substring(7, 7) == ""`},
    // Trim tests using the unicode standard for whitespace.
    {expr: `" \f\n\r\t\vtext  ".trim() == "text"`},
    {expr: `"\u0085\u00a0\u1680text".trim() == "text"`},
    {expr: `"text\u2000\u2001\u2002\u2003\u2004\u2004\u2006\u2007\u2008\u2009".trim() == "text"`},
    {expr: `"\u200atext\u2028\u2029\u202F\u205F\u3000".trim() == "text"`},
    // Trim test with whitespace-like characters not included.
    {expr: `"\u180etext\u200b\u200c\u200d\u2060\ufeff".trim()
                == "\u180etext\u200b\u200c\u200d\u2060\ufeff"`},
    // Upper ASCII tests.
    {expr: `'tacoCat'.upperAscii() == 'TACOCAT'`},
    {expr: `'tacoCαt'.upperAscii() == 'TACOCαT'`},
    // Reverse tests.
    {expr: `'gums'.reverse() == 'smug'`},
    {expr: `'palindromes'.reverse() == 'semordnilap'`},
    {expr: `'John Smith'.reverse() == 'htimS nhoJ'`},
    {expr: `'u180etext'.reverse() == 'txete081u'`},
    {expr: `'2600+U'.reverse() == 'U+0062'`},
    {expr: `'\u180e\u200b\u200c\u200d\u2060\ufeff'.reverse() == '\ufeff\u2060\u200d\u200c\u200b\u180e'`},
    // Join tests.
    {expr: `['x', 'y'].join() == 'xy'`},
    {expr: `['x', 'y'].join('-') == 'x-y'`},
    {expr: `[].join() == ''`},
    {expr: `[].join('-') == ''`},
    // Escaping tests.
    {expr: `strings.quote("first\nsecond") == "\"first\\nsecond\""`},
    {expr: `strings.quote("bell\a") == "\"bell\\a\""`},
    {expr: `strings.quote("\bbackspace") == "\"\\bbackspace\""`},
    {expr: `strings.quote("\fform feed") == "\"\\fform feed\""`},
    {expr: `strings.quote("carriage \r return") == "\"carriage \\r return\""`},
    {expr: `strings.quote("horizontal tab\t") == "\"horizontal tab\\t\""`},
    {expr: `strings.quote("vertical \v tab") == "\"vertical \\v tab\""`},
    {expr: `strings.quote("double \\\\ slash") == "\"double \\\\\\\\ slash\""`},
    {expr: `strings.quote("two escape sequences \a\n") == "\"two escape sequences \\a\\n\""`},
    {expr: `strings.quote("verbatim") == "\"verbatim\""`},
    {expr: `strings.quote("ends with \\") == "\"ends with \\\\\""`},
    {expr: `strings.quote("\\ starts with") == "\"\\\\ starts with\""`},
    {expr: `strings.quote("printable unicode😀") == "\"printable unicode😀\""`},
    {expr: `strings.quote("mid string \" quote") == "\"mid string \\\" quote\""`},
    {expr: `strings.quote('single-quote with "double quote"') == "\"single-quote with \\\"double quote\\\"\""`},
    {expr: `strings.quote("size('ÿ')") == "\"size('ÿ')\""`},
    {expr: `strings.quote("size('πέντε')") == "\"size('πέντε')\""`},
    {expr: `strings.quote("завтра") == "\"завтра\""`},
    {expr: `strings.quote("\U0001F431\U0001F600\U0001F61B") == "\"\U0001F431\U0001F600\U0001F61B\""`},
    {expr: `strings.quote("ta©o©αT") == "\"ta©o©αT\""`},
    {expr: `strings.quote("") == "\"\""`},
    // Error test cases based on checked expression usage.
    {
        expr: `'tacocat'.charAt(30) == ''`,
        err:  "index out of range: 30",
    },
    {
        expr: `'tacocat'.indexOf('a', 30) == -1`,
        err:  "index out of range: 30",
    },
    {
        expr: `'tacocat'.lastIndexOf('a', -1) == -1`,
        err:  "index out of range: -1",
    },
    {
        expr: `'tacocat'.lastIndexOf('a', 30) == -1`,
        err:  "index out of range: 30",
    },
    {
        expr: `"tacocat".substring(40) == "cat"`,
        err:  "index out of range: 40",
    },
    {
        expr: `"tacocat".substring(-1) == "cat"`,
        err:  "index out of range: -1",
    },
    {
        expr: `"tacocat".substring(1, 50) == "cat"`,
        err:  "index out of range: 50",
    },
    {
        expr: `"tacocat".substring(49, 50) == "cat"`,
        err:  "index out of range: 49",
    },
    {
        expr: `"tacocat".substring(4, 3) == ""`,
        err:  "invalid substring range. start: 4, end: 3",
    },
    // Valid parse-only expressions which should generate runtime errors.
    {
        expr:      `42.charAt(2) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'hello'.charAt(true) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `24.indexOf('2') == 0`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'hello'.indexOf(true) == 1`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.indexOf('4', 0) == 0`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'42'.indexOf(4, 0) == 0`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'42'.indexOf('4', '0') == 0`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'42'.indexOf('4', 0, 1) == 0`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.split("2") == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.replace(2, 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace(2, 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace("2", 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.replace("2", "1", 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace(2, "1", 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace("2", 1, 1) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace("2", "1", "1") == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".replace("2", "1", 1, false) == "41"`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.split("") == ["4", "2"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".split(2) == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `42.split("2", "1") == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".split(2, 1) == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".split("2", "1") == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"42".split("2", 1, 1) == ["4"]`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `'hello'.substring(1, 2, 3) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `30.substring(true, 3) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"tacocat".substring(true, 3) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
    {
        expr:      `"tacocat".substring(0, false) == ""`,
        err:       "no such overload",
        parseOnly: true,
    },
}

func TestStrings(t *testing.T) {
    env, err := cel.NewEnv(Strings())
    if err != nil {
        t.Fatalf("cel.NewEnv(Strings()) failed: %v", err)
    }
    for i, tst := range stringTests {
        tc := tst
        t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
            var asts []*cel.Ast
            pAst, iss := env.Parse(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Parse(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, pAst)
            if !tc.parseOnly {
                cAst, iss := env.Check(pAst)
                if iss.Err() != nil {
                    t.Fatalf("env.Check(%v) failed: %v", tc.expr, iss.Err())
                }
                asts = append(asts, cAst)
            }
            for _, ast := range asts {
                prg, err := env.Program(ast)
                if err != nil {
                    t.Fatal(err)
                }
                out, _, err := prg.Eval(cel.NoVars())
                if tc.err != "" {
                    if err == nil {
                        t.Fatalf("got value %v, wanted error %s for expr: %s",
                            out.Value(), tc.err, tc.expr)
                    }
                    if !strings.Contains(err.Error(), tc.err) {
                        t.Errorf("got %q, expected error to contain %q for expr: %s", err, tc.err, tc.expr)
                    }
                } else if err != nil {
                    t.Fatal(err)
                } else if out.Value() != true {
                    t.Errorf("got %v, wanted true for expr: %s", out.Value(), tc.expr)
                }
            }
        })
    }
}

func TestVersions(t *testing.T) {
    versionCases := []struct {
        version            uint32
        supportedFunctions map[string]string
    }{
        {
            version: 0,
            supportedFunctions: map[string]string{
                "chatAt":      "''.charAt(0) == ''",
                "indexOf":     "'a'.indexOf('a') == 0",
                "lastIndexOf": "'a'.lastIndexOf('a') == 0",
                "join":        "['a', 'b'].join() == 'ab'",
                "joinSep":     "['a', 'b'].join('-') == 'a-b'",
                "lowerAscii":  "'a'.lowerAscii() == 'a'",
                "replace":     "'hello hello'.replace('he', 'we') == 'wello wello'",
                "split":       "'hello hello hello'.split(' ') == ['hello', 'hello', 'hello']",
                "substring":   "'tacocat'.substring(4) == 'cat'",
                "trim":        "'  \\ttrim\\n    '.trim() == 'trim'",
                "upperAscii":  "'TacoCat'.upperAscii() == 'TACOCAT'",
            },
        },
        {
            version: 1,
            supportedFunctions: map[string]string{
                "format": "'a %d'.format([1]) == 'a 1'",
                "quote":  `strings.quote('\a \b "double quotes"') == '"\\a \\b \\"double quotes\\""'`,
            },
        },
    }
    for _, lib := range versionCases {
        env, err := cel.NewEnv(Strings(StringsVersion(lib.version)))
        if err != nil {
            t.Fatalf("cel.NewEnv(Strings(StringsVersion(%d))) failed: %v", lib.version, err)
        }
        t.Run(fmt.Sprintf("version=%d", lib.version), func(t *testing.T) {
            for _, tc := range versionCases {
                for name, expr := range tc.supportedFunctions {
                    supported := lib.version >= tc.version
                    t.Run(fmt.Sprintf("%s-supported=%t", name, supported), func(t *testing.T) {
                        ast, iss := env.Compile(expr)
                        if supported {
                            if iss.Err() != nil {
                                t.Errorf("unexpected error: %v", iss.Err())
                            }
                        } else {
                            if iss.Err() == nil || !strings.Contains(iss.Err().Error(), "undeclared reference") {
                                t.Errorf("got error %v, wanted error %s for expr: %s, version: %d", iss.Err(), "undeclared reference", expr, tc.version)
                            }
                            return
                        }
                        prg, err := env.Program(ast)
                        if err != nil {
                            t.Fatalf("env.Program() failed: %v", err)
                        }
                        out, _, err := prg.Eval(cel.NoVars())
                        if err != nil {
                            t.Fatalf("prg.Eval() failed: %v", err)
                        }
                        if out != types.True {
                            t.Errorf("prg.Eval() got %v, wanted true", out)
                        }
                    })
                }
            }
        })
    }
}

func version(v uint32) *uint32 {
    return &v
}

func TestStringsWithExtension(t *testing.T) {
    env, err := cel.NewEnv(Strings())
    if err != nil {
        t.Fatalf("cel.NewEnv(Strings()) failed: %v", err)
    }
    _, err = env.Extend(Strings())
    if err != nil {
        t.Fatalf("env.Extend(Strings()) failed: %v", err)
    }
}

func TestStringFormat(t *testing.T) {
    tests := []struct {
        name                  string
        format                string
        dynArgs               map[string]any
        formatArgs            string
        locale                string
        err                   string
        expectedOutput        string
        expectedRuntimeCost   uint64
        expectedEstimatedCost checker.CostEstimate
        skipCompileCheck      bool
    }{
        {
            name:                  "no-op",
            format:                "no substitution",
            expectedOutput:        "no substitution",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },

        {
            name:                  "mid-string substitution",
            format:                "str is %s and some more",
            formatArgs:            `"filler"`,
            expectedOutput:        "str is filler and some more",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:                  "percent escaping",
            format:                "%% and also %%",
            expectedOutput:        "% and also %",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "substution inside escaped percent signs",
            format:                "%%%s%%",
            formatArgs:            `"text"`,
            expectedOutput:        "%text%",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "substitution with one escaped percent sign on the right",
            format:                "%s%%",
            formatArgs:            `"percent on the right"`,
            expectedOutput:        "percent on the right%",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "substitution with one escaped percent sign on the left",
            format:                "%%%s",
            formatArgs:            `"percent on the left"`,
            expectedOutput:        "%percent on the left",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "multiple substitutions",
            format:                "%d %d %d, %s %s %s, %d %d %d, %s %s %s",
            formatArgs:            `1, 2, 3, "A", "B", "C", 4, 5, 6, "D", "E", "F"`,
            expectedOutput:        "1 2 3, A B C, 4 5 6, D E F",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:                  "percent sign escape sequence support",
            format:                "\u0025\u0025escaped \u0025s\u0025\u0025",
            formatArgs:            `"percent"`,
            expectedOutput:        "%escaped percent%",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "fixed point formatting clause",
            format:                "%.3f",
            formatArgs:            "1.2345",
            expectedOutput:        "1.234",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "binary formatting clause",
            format:                "this is 5 in binary: %b",
            formatArgs:            "5",
            expectedOutput:        "this is 5 in binary: 101",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:                  "uint support for binary formatting",
            format:                "unsigned 64 in binary: %b",
            formatArgs:            "uint(64)",
            expectedOutput:        "unsigned 64 in binary: 1000000",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:                  "bool support for binary formatting",
            format:                "bit set from bool: %b",
            formatArgs:            "true",
            expectedOutput:        "bit set from bool: 1",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:                  "octal formatting clause",
            format:                "%o",
            formatArgs:            "11",
            expectedOutput:        "13",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "uint support for octal formatting clause",
            format:                "this is an unsigned octal: %o",
            formatArgs:            "uint(65535)",
            expectedOutput:        "this is an unsigned octal: 177777",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:                  "lowercase hexadecimal formatting clause",
            format:                "%x is 20 in hexadecimal",
            formatArgs:            "30",
            expectedOutput:        "1e is 20 in hexadecimal",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:                  "uppercase hexadecimal formatting clause",
            format:                "%X is 20 in hexadecimal",
            formatArgs:            "30",
            expectedOutput:        "1E is 20 in hexadecimal",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:                  "unsigned support for hexadecimal formatting clause",
            format:                "%X is 6000 in hexadecimal",
            formatArgs:            "uint(6000)",
            expectedOutput:        "1770 is 6000 in hexadecimal",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:                  "string support with hexadecimal formatting clause",
            format:                "%x",
            formatArgs:            `"Hello world!"`,
            expectedOutput:        "48656c6c6f20776f726c6421",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "string support with uppercase hexadecimal formatting clause",
            format:                "%X",
            formatArgs:            `"Hello world!"`,
            expectedOutput:        "48656C6C6F20776F726C6421",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "byte support with hexadecimal formatting clause",
            format:                "%x",
            formatArgs:            `b"byte string"`,
            expectedOutput:        "6279746520737472696e67",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "byte support with uppercase hexadecimal formatting clause",
            format:                "%X",
            formatArgs:            `b"byte string"`,
            expectedOutput:        "6279746520737472696E67",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "scientific notation formatting clause",
            format:                "%.6e",
            formatArgs:            "1052.032911275",
            expectedOutput:        "1.052033\u202f\u00d7\u202f10\u2070\u00b3",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "locale support",
            format:                "%.3f",
            formatArgs:            "3.14",
            locale:                "fr_FR",
            expectedOutput:        "3,140",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "default precision for fixed-point clause",
            format:                "%f",
            formatArgs:            "2.71828",
            expectedOutput:        "2.718280",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "default precision for scientific notation",
            format:                "%e",
            formatArgs:            "2.71828",
            expectedOutput:        "2.718280\u202f\u00d7\u202f10\u2070\u2070",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "unicode output for scientific notation",
            format:                "unescaped unicode: %e, escaped unicode: %e",
            formatArgs:            "2.71828, 2.71828",
            expectedOutput:        "unescaped unicode: 2.718280 × 10⁰⁰, escaped unicode: 2.718280\u202f\u00d7\u202f10\u2070\u2070",
            expectedRuntimeCost:   15,
            expectedEstimatedCost: checker.CostEstimate{Min: 15, Max: 15},
            locale:                "en_US",
        },
        {
            name:                  "NaN support for fixed-point",
            format:                "%f",
            formatArgs:            `"NaN"`,
            expectedOutput:        "NaN",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "positive infinity support for fixed-point",
            format:                "%f",
            formatArgs:            `"Infinity"`,
            expectedOutput:        "∞",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "negative infinity support for fixed-point",
            format:                "%f",
            formatArgs:            `"-Infinity"`,
            expectedOutput:        "-∞",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
            locale:                "en_US",
        },
        {
            name:                  "uint support for decimal clause",
            format:                "%d",
            formatArgs:            "uint(64)",
            expectedOutput:        "64",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "null support for string",
            format:                "null: %s",
            formatArgs:            "null",
            expectedOutput:        "null: null",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "int support for string",
            format:                "%s",
            formatArgs:            `999999999999`,
            expectedOutput:        "999999999999",
            expectedRuntimeCost:   11,
            expectedEstimatedCost: checker.CostEstimate{Min: 11, Max: 11},
        },
        {
            name:                  "bytes support for string",
            format:                "some bytes: %s",
            formatArgs:            `b"xyz"`,
            expectedOutput:        "some bytes: xyz",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "type() support for string",
            format:                "type is %s",
            formatArgs:            `type("test string")`,
            expectedOutput:        "type is string",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "timestamp support for string",
            format:                "%s",
            formatArgs:            `timestamp("2023-02-03T23:31:20+00:00")`,
            expectedOutput:        "2023-02-03T23:31:20Z",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "duration support for string",
            format:                "%s",
            formatArgs:            `duration("1h45m47s")`,
            expectedOutput:        "6347s",
            expectedRuntimeCost:   12,
            expectedEstimatedCost: checker.CostEstimate{Min: 12, Max: 12},
        },
        {
            name:                  "list support for string",
            format:                "%s",
            formatArgs:            `["abc", 3.14, null, [9, 8, 7, 6], timestamp("2023-02-03T23:31:20Z")]`,
            expectedOutput:        `["abc", 3.140000, null, [9, 8, 7, 6], timestamp("2023-02-03T23:31:20Z")]`,
            expectedRuntimeCost:   32,
            expectedEstimatedCost: checker.CostEstimate{Min: 32, Max: 32},
        },
        {
            name:                  "map support for string",
            format:                "%s",
            formatArgs:            `{"key1": b"xyz", "key5": null, "key2": duration("2h"), "key4": true, "key3": 2.71828}`,
            locale:                "nl_NL",
            expectedOutput:        `{"key1":b"xyz", "key2":duration("7200s"), "key3":2.718280, "key4":true, "key5":null}`,
            expectedRuntimeCost:   42,
            expectedEstimatedCost: checker.CostEstimate{Min: 42, Max: 42},
        },
        {
            name:                  "map support (all key types)",
            format:                "map with multiple key types: %s",
            formatArgs:            `{1: "value1", uint(2): "value2", true: double("NaN")}`,
            expectedOutput:        `map with multiple key types: {1:"value1", 2:"value2", true:"NaN"}`,
            expectedRuntimeCost:   46,
            expectedEstimatedCost: checker.CostEstimate{Min: 46, Max: 46},
        },
        {
            name:                  "boolean support for %s",
            format:                "true bool: %s, false bool: %s",
            formatArgs:            `true, false`,
            expectedOutput:        "true bool: true, false bool: false",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for string formatting clause",
            format:     "dynamic string: %s",
            formatArgs: `dynStr`,
            dynArgs: map[string]any{
                "dynStr": "a string",
            },
            expectedOutput:        "dynamic string: a string",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for numbers with string formatting clause",
            format:     "dynIntStr: %s dynDoubleStr: %s",
            formatArgs: `dynIntStr, dynDoubleStr`,
            dynArgs: map[string]any{
                "dynIntStr":    32,
                "dynDoubleStr": 56.8,
            },
            expectedOutput:        "dynIntStr: 32 dynDoubleStr: 56.8",
            expectedRuntimeCost:   15,
            expectedEstimatedCost: checker.CostEstimate{Min: 15, Max: 15},
            locale:                "en_US",
        },
        {
            name:       "dyntype support for integer formatting clause",
            format:     "dynamic int: %d",
            formatArgs: `dynInt`,
            dynArgs: map[string]any{
                "dynInt": 128,
            },
            expectedOutput:        "dynamic int: 128",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for integer formatting clause (unsigned)",
            format:     "dynamic unsigned int: %d",
            formatArgs: `dynUnsignedInt`,
            dynArgs: map[string]any{
                "dynUnsignedInt": uint64(256),
            },
            expectedOutput:        "dynamic unsigned int: 256",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:       "dyntype support for hex formatting clause",
            format:     "dynamic hex int: %x",
            formatArgs: `dynHexInt`,
            dynArgs: map[string]any{
                "dynHexInt": 22,
            },
            expectedOutput:        "dynamic hex int: 16",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for hex formatting clause (uppercase)",
            format:     "dynamic hex int: %X (uppercase)",
            formatArgs: `dynHexInt`,
            dynArgs: map[string]any{
                "dynHexInt": 26,
            },
            expectedOutput:        "dynamic hex int: 1A (uppercase)",
            expectedRuntimeCost:   15,
            expectedEstimatedCost: checker.CostEstimate{Min: 15, Max: 15},
        },
        {
            name:       "dyntype support for unsigned hex formatting clause",
            format:     "dynamic hex int: %x (unsigned)",
            formatArgs: `dynUnsignedHexInt`,
            dynArgs: map[string]any{
                "dynUnsignedHexInt": uint(500),
            },
            expectedOutput:        "dynamic hex int: 1f4 (unsigned)",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:       "dyntype support for fixed-point formatting clause",
            format:     "dynamic double: %.3f",
            formatArgs: `dynDouble`,
            dynArgs: map[string]any{
                "dynDouble": 4.5,
            },
            expectedOutput:        "dynamic double: 4.500",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
            locale:                "en_US",
        },
        {
            name:       "dyntype support for fixed-point formatting clause (comma separator locale)",
            format:     "dynamic double: %f",
            formatArgs: `dynDouble`,
            dynArgs: map[string]any{
                "dynDouble": 4.5,
            },
            expectedOutput:        "dynamic double: 4,500000",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
            locale:                "fr_FR",
        },
        {
            name:       "dyntype support for scientific notation",
            format:     "(dyntype) e: %e",
            formatArgs: "dynE",
            dynArgs: map[string]any{
                "dynE": 2.71828,
            },
            expectedOutput:        "(dyntype) e: 2.718280\u202f\u00d7\u202f10\u2070\u2070",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
            locale:                "en_US",
        },
        {
            name:       "dyntype NaN/infinity support for fixed-point",
            format:     "NaN: %f, infinity: %f",
            formatArgs: `dynNaN, dynInf`,
            dynArgs: map[string]any{
                "dynNaN": math.NaN(),
                "dynInf": math.Inf(1),
            },
            expectedOutput:        "NaN: NaN, infinity: ∞",
            expectedRuntimeCost:   15,
            expectedEstimatedCost: checker.CostEstimate{Min: 15, Max: 15},
        },
        {
            name:       "dyntype support for timestamp",
            format:     "dyntype timestamp: %s",
            formatArgs: `dynTime`,
            dynArgs: map[string]any{
                "dynTime": time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
            },
            expectedOutput:        "dyntype timestamp: 2009-11-10T23:00:00Z",
            expectedRuntimeCost:   14,
            expectedEstimatedCost: checker.CostEstimate{Min: 14, Max: 14},
        },
        {
            name:       "dyntype support for duration",
            format:     "dyntype duration: %s",
            formatArgs: `dynDuration`,
            dynArgs: map[string]any{
                "dynDuration": mustParseDuration("2h25m47s"),
            },
            expectedOutput:        "dyntype duration: 8747s",
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for lists",
            format:     "dyntype list: %s",
            formatArgs: `dynList`,
            dynArgs: map[string]any{
                "dynList": []any{6, 4.2, "a string"},
            },
            expectedOutput:        `dyntype list: [6, 4.200000, "a string"]`,
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "dyntype support for maps",
            format:     "dyntype map: %s",
            formatArgs: `dynMap`,
            dynArgs: map[string]any{
                "dynMap": map[any]any{
                    "strKey": "x",
                    true:     42,
                    int64(6): mustParseDuration("7m2s"),
                },
            },
            expectedOutput:        `dyntype map: {"strKey":"x", 6:duration("422s"), true:42}`,
            expectedRuntimeCost:   13,
            expectedEstimatedCost: checker.CostEstimate{Min: 13, Max: 13},
        },
        {
            name:       "message field support",
            format:     "message field msg.single_int32: %d, msg.single_double: %.1f",
            formatArgs: `msg.single_int32, msg.single_double`,
            dynArgs: map[string]any{
                "msg": &proto3pb.TestAllTypes{
                    SingleInt32:  2,
                    SingleDouble: 1.0,
                },
            },
            locale:         "en_US",
            expectedOutput: `message field msg.single_int32: 2, msg.single_double: 1.0`,
        },
        {
            name:             "unrecognized formatting clause",
            format:           "%a",
            formatArgs:       "1",
            skipCompileCheck: true,
            err:              "could not parse formatting clause: unrecognized formatting clause \"a\"",
        },
        {
            name:             "out of bounds arg index",
            format:           "%d %d %d",
            formatArgs:       "0, 1",
            skipCompileCheck: true,
            err:              "index 2 out of range",
        },
        {
            name:             "string substitution is not allowed with binary clause",
            format:           "string is %b",
            formatArgs:       `"abc"`,
            skipCompileCheck: true,
            err:              "error during formatting: only integers and bools can be formatted as binary, was given string",
        },
        {
            name:             "duration substitution not allowed with decimal clause",
            format:           "%d",
            formatArgs:       `duration("30m2s")`,
            skipCompileCheck: true,
            err:              "error during formatting: decimal clause can only be used on integers, was given google.protobuf.Duration",
        },
        {
            name:             "string substitution not allowed with octal clause",
            format:           "octal: %o",
            formatArgs:       `"a string"`,
            skipCompileCheck: true,
            err:              "error during formatting: octal clause can only be used on integers, was given string",
        },
        {
            name:             "double substitution not allowed with hex clause",
            format:           "double is %x",
            formatArgs:       "0.5",
            skipCompileCheck: true,
            err:              "error during formatting: only integers, byte buffers, and strings can be formatted as hex, was given double",
        },
        {
            name:             "uppercase not allowed for scientific clause",
            format:           "double is %E",
            formatArgs:       "0.5",
            skipCompileCheck: true,
            err:              `could not parse formatting clause: unrecognized formatting clause "E"`,
        },
        {
            name:             "object not allowed",
            format:           "object is %s",
            formatArgs:       `ext.TestAllTypes{PbVal: test.TestAllTypes{}}`,
            skipCompileCheck: true,
            err:              "error during formatting: string clause can only be used on strings, bools, bytes, ints, doubles, maps, lists, types, durations, and timestamps, was given ext.TestAllTypes",
        },
        {
            name:             "object inside list",
            format:           "%s",
            formatArgs:       "[1, 2, ext.TestAllTypes{PbVal: test.TestAllTypes{}}]",
            skipCompileCheck: true,
            err:              "error during formatting: string clause can only be used on strings, bools, bytes, ints, doubles, maps, lists, types, durations, and timestamps, was given ext.TestAllTypes",
        },
        {
            name:             "object inside map",
            format:           "%s",
            formatArgs:       `{1: "a", 2: ext.TestAllTypes{}}`,
            skipCompileCheck: true,
            err:              "error during formatting: string clause can only be used on strings, bools, bytes, ints, doubles, maps, lists, types, durations, and timestamps, was given ext.TestAllTypes",
        },
        {
            name:             "null not allowed for %d",
            format:           "null: %d",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: decimal clause can only be used on integers, was given null_type",
        },
        {
            name:             "null not allowed for %e",
            format:           "null: %e",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: scientific clause can only be used on doubles, was given null_type",
        },
        {
            name:             "null not allowed for %f",
            format:           "null: %f",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: fixed-point clause can only be used on doubles, was given null_type",
        },
        {
            name:             "null not allowed for %x",
            format:           "null: %x",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: only integers, byte buffers, and strings can be formatted as hex, was given null_type",
        },
        {
            name:             "null not allowed for %X",
            format:           "null: %X",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: only integers, byte buffers, and strings can be formatted as hex, was given null_type",
        },
        {
            name:             "null not allowed for %b",
            format:           "null: %b",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: only integers and bools can be formatted as binary, was given null_type",
        },
        {
            name:             "null not allowed for %o",
            format:           "null: %o",
            formatArgs:       "null",
            skipCompileCheck: true,
            err:              "error during formatting: octal clause can only be used on integers, was given null_type",
        },
        {
            name:       "compile-time cardinality check (too few for string)",
            format:     "%s %s",
            formatArgs: `"abc"`,
            err:        "index 1 out of range",
        },
        {
            name:       "compile-time cardinality check (too many for string)",
            format:     "%s %s",
            formatArgs: `"abc", "def", "ghi"`,
            err:        "too many arguments supplied to string.format (expected 2, got 3)",
        },
        {
            name:       "compile-time syntax check (unexpected end of string)",
            format:     "filler %",
            formatArgs: "",
            err:        "unexpected end of string",
        },
        {
            name:   "compile-time syntax check (unrecognized formatting clause)",
            format: "%j",
            // pass args here, otherwise the cardinality check will fail first
            formatArgs: "123",
            err:        `could not parse formatting clause: unrecognized formatting clause "j"`,
        },
        {
            name:       "compile-time %s check",
            format:     "object is %s",
            formatArgs: `ext.TestAllTypes{PbVal: test.TestAllTypes{}}`,
            err:        "error during formatting: string clause can only be used on strings, bools, bytes, ints, doubles, maps, lists, types, durations, and timestamps",
        },
        {
            name:       "compile-time check for objects inside list literal",
            format:     "list is %s",
            formatArgs: `[1, 2, ext.TestAllTypes{PbVal: test.TestAllTypes{}}]`,
            err:        "error during formatting: string clause can only be used on strings, bools, bytes, ints, doubles, maps, lists, types, durations, and timestamps",
        },
        {
            name:       "compile-time %d check",
            format:     "int is %d",
            formatArgs: "5.2",
            err:        "error during formatting: decimal clause can only be used on integers",
        },
        {
            name:       "compile-time %f check",
            format:     "double is %f",
            formatArgs: "true",
            err:        "error during formatting: fixed-point clause can only be used on doubles",
        },
        {
            name:       "compile-time precision syntax check",
            format:     "double is %.34",
            formatArgs: "5.0",
            err:        "could not parse formatting clause: error while parsing precision: could not find end of precision specifier",
        },
        {
            name:       "compile-time %e check",
            format:     "double is %e",
            formatArgs: "true",
            err:        "error during formatting: scientific clause can only be used on doubles",
        },
        {
            name:       "compile-time %b check",
            format:     "string is %b",
            formatArgs: `"a string"`,
            err:        "error during formatting: only integers and bools can be formatted as binary",
        },
        {
            name:       "compile-time %x check",
            format:     "%x is a double",
            formatArgs: "2.5",
            err:        "error during formatting: only integers, byte buffers, and strings can be formatted as hex",
        },
        {
            name:       "compile-time %X check",
            format:     "%X is a double",
            formatArgs: "2.5",
            err:        "error during formatting: only integers, byte buffers, and strings can be formatted as hex",
        },
        {
            name:       "compile-time %o check",
            format:     "an octal: %o",
            formatArgs: "3.14",
            err:        "error during formatting: octal clause can only be used on integers",
        },
    }
    evalExpr := func(env *cel.Env, expr string, evalArgs any, expectedRuntimeCost uint64, expectedEstimatedCost checker.CostEstimate, t *testing.T) (ref.Val, error) {
        t.Logf("evaluating expr: %s", expr)
        parsedAst, issues := env.Parse(expr)
        if issues.Err() != nil {
            t.Fatalf("env.Parse(%v) failed: %v", expr, issues.Err())
        }
        checkedAst, issues := env.Check(parsedAst)
        if issues.Err() != nil {
            return nil, issues.Err()
        }
        evalOpts := make([]cel.ProgramOption, 0)
        costTracker := &noopCostEstimator{}
        if expectedRuntimeCost != 0 {
            evalOpts = append(evalOpts, cel.CostTracking(costTracker))
        }
        program, err := env.Program(checkedAst, evalOpts...)
        if err != nil {
            return nil, err
        }

        actualEstimatedCost, err := env.EstimateCost(checkedAst, costTracker)
        if err != nil {
            t.Fatal(err)
        }
        if expectedEstimatedCost.Min != 0 && expectedEstimatedCost.Max != 0 {
            if actualEstimatedCost.Min != expectedEstimatedCost.Min && actualEstimatedCost.Max != expectedEstimatedCost.Max {
                t.Fatalf("expected estimated cost range to be %v, was %v", expectedEstimatedCost, actualEstimatedCost)
            }
        }

        var out ref.Val
        var details *cel.EvalDetails
        if evalArgs != nil {
            out, details, err = program.Eval(evalArgs)
        } else {
            out, details, err = program.Eval(cel.NoVars())
        }

        if expectedRuntimeCost != 0 {
            if details == nil {
                t.Fatal("no EvalDetails available when runtime cost was expected")
            }
            if *details.ActualCost() != expectedRuntimeCost {
                t.Fatalf("expected runtime cost to be %d, was %d", expectedRuntimeCost, *details.ActualCost())
            }
            if expectedEstimatedCost.Min != 0 && expectedEstimatedCost.Max != 0 {
                if *details.ActualCost() < expectedEstimatedCost.Min || *details.ActualCost() > expectedEstimatedCost.Max {
                    t.Fatalf("runtime cost %d outside of expected estimated cost range %v", *details.ActualCost(), expectedEstimatedCost)
                }
            }
        }
        return out, err
    }
    buildVariables := func(vars map[string]any) []cel.EnvOption {
        opts := make([]cel.EnvOption, len(vars))
        i := 0
        for name, value := range vars {
            t := cel.DynType
            switch v := value.(type) {
            case proto.Message:
                t = cel.ObjectType(string(v.ProtoReflect().Descriptor().FullName()))
            case types.Bool:
                t = cel.BoolType
            case types.Bytes:
                t = cel.BytesType
            case types.Double:
                t = cel.DoubleType
            case types.Duration:
                t = cel.DurationType
            case types.Int:
                t = cel.IntType
            case types.Null:
                t = cel.NullType
            case types.String:
                t = cel.StringType
            case types.Timestamp:
                t = cel.TimestampType
            case types.Uint:
                t = cel.UintType
            }
            opts[i] = cel.Variable(name, t)
            i++
        }
        return opts
    }
    buildOpts := func(skipCompileCheck bool, locale string, variables []cel.EnvOption) []cel.EnvOption {
        opts := []cel.EnvOption{
            Strings(StringsLocale(locale), StringsValidateFormatCalls(!skipCompileCheck)),
            cel.Container("ext"),
            cel.Abbrevs("google.expr.proto3.test"),
            cel.Types(&proto3pb.TestAllTypes{}),
            NativeTypes(
                reflect.TypeOf(&TestNestedType{}),
                reflect.ValueOf(&TestAllTypes{}),
            ),
        }
        opts = append(opts, cel.ASTValidators(cel.ValidateHomogeneousAggregateLiterals()))
        opts = append(opts, variables...)
        return opts
    }
    runCase := func(format, formatArgs, locale string, dynArgs map[string]any, skipCompileCheck bool, expectedRuntimeCost uint64, expectedEstimatedCost checker.CostEstimate, t *testing.T) (ref.Val, error) {
        env, err := cel.NewEnv(buildOpts(skipCompileCheck, locale, buildVariables(dynArgs))...)
        if err != nil {
            t.Fatalf("cel.NewEnv() failed: %v", err)
        }
        expr := fmt.Sprintf("%q.format([%s])", format, formatArgs)
        if len(dynArgs) == 0 {
            return evalExpr(env, expr, cel.NoVars(), expectedRuntimeCost, expectedEstimatedCost, t)
        }
        return evalExpr(env, expr, dynArgs, expectedRuntimeCost, expectedEstimatedCost, t)
    }
    checkCase := func(output ref.Val, expectedOutput string, err error, expectedErr string, t *testing.T) {
        if err != nil {
            if expectedErr != "" {
                if !strings.Contains(err.Error(), expectedErr) {
                    t.Fatalf("expected %q as error message, got %q", expectedErr, err.Error())
                }
            } else {
                t.Fatalf("unexpected error: %s", err)
            }
        } else {
            if output.Type() != types.StringType {
                t.Fatalf("expected test expr to eval to string (got %s instead)", output.Type().TypeName())
            } else {
                outputStr := output.Value().(string)
                if outputStr != expectedOutput {
                    t.Errorf("expected %q as output, got %q", expectedOutput, outputStr)
                }
            }
        }
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            out, err := runCase(tt.format, tt.formatArgs, tt.locale, tt.dynArgs, tt.skipCompileCheck, tt.expectedRuntimeCost, tt.expectedEstimatedCost, t)
            checkCase(out, tt.expectedOutput, err, tt.err, t)
            if tt.locale == "" {
                // if the test has no locale specified, then that means it
                // should have the same output regardless of locale
                t.Run("no change on locale", func(t *testing.T) {
                    out, err := runCase(tt.format, tt.formatArgs, "da_DK", tt.dynArgs, tt.skipCompileCheck, tt.expectedRuntimeCost, tt.expectedEstimatedCost, t)
                    checkCase(out, tt.expectedOutput, err, tt.err, t)
                })
            }
        })
    }
}

func TestStringFormatHeterogeneousLiterals(t *testing.T) {
    tests := []struct {
        expr string
        out  string
    }{
        {
            expr: `"list: %s".format([[[1, 2, [3.0, 4]]]])`,
            out:  `list: [[1, 2, [3.000000, 4]]]`,
        },
        {
            expr: `"list size: %d".format([[[1, 2, [3.0, 4]]].size()])`,
            out:  `list size: 1`,
        },
        {
            expr: `"list element: %s".format([[[1, 2, [3.0, 4]]][0]])`,
            out:  `list element: [1, 2, [3.000000, 4]]`,
        },
    }
    env, err := cel.NewEnv(Strings(), cel.ASTValidators(cel.ValidateHomogeneousAggregateLiterals()))
    if err != nil {
        t.Fatalf("cel.NewEnv() failed: %v", err)
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            ast, iss := env.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
            }
            prg, err := env.Program(ast)
            if err != nil {
                t.Fatalf("env.Program() failed: %v", err)
            }
            out, _, err := prg.Eval(cel.NoVars())
            if err != nil {
                t.Fatalf("Eval() failed: %v", err)
            }
            if out.Value() != tc.out {
                t.Errorf("Eval() got %v, wanted %v", out, tc.out)
            }
        })
    }
}

func TestBadLocale(t *testing.T) {
    _, err := cel.NewEnv(Strings(StringsLocale("bad-locale")))
    if err != nil {
        if err.Error() != "failed to parse locale: language: subtag \"locale\" is well-formed but unknown" {
            t.Errorf("expected error messaged to be \"failed to parse locale: language: subtag \"locale\" is well-formed but unknown\", got %q", err)
        }
    } else {
        t.Error("expected NewEnv to fail during locale parsing")
    }
}

func TestLiteralOutput(t *testing.T) {
    tests := []struct {
        name          string
        formatLiteral string
        expectedType  string
    }{
        {
            name:          "map literal support",
            formatLiteral: `{"key1": b"xyz", false: [11, 12, 13, timestamp("2019-10-12T07:20:50.52Z")], 42: {uint(64): 2.7}, "key5": type(int), "key2": duration("2h"), "key4": true, "key3": 2.71828, "null": null}`,
            expectedType:  `map`,
        },
        {
            name:          "list literal support",
            formatLiteral: `["abc", 3.14, uint(32), b"def", null, type(string), duration("7m"), [9, 8, 7, 6], timestamp("2023-02-03T23:31:20Z")]`,
            expectedType:  `list`,
        },
    }
    for _, tt := range tests {
        parseAndEval := func(expr string, t *testing.T) (ref.Val, error) {
            env, err := cel.NewEnv(Strings())
            if err != nil {
                t.Fatalf("cel.NewEnv(Strings()) failed: %v", err)
            }
            parsedAst, issues := env.Parse(expr)
            if issues.Err() != nil {
                t.Fatalf("env.Parse(%v) failed: %v", expr, issues.Err())
            }
            checkedAst, issues := env.Check(parsedAst)
            if issues.Err() != nil {
                t.Fatalf("env.Check(%v) failed: %v", expr, issues.Err())
            }
            program, err := env.Program(checkedAst)
            if err != nil {
                t.Fatal(err)
            }
            out, _, err := program.Eval(cel.NoVars())
            return out, err
        }
        t.Run(tt.name, func(t *testing.T) {
            expr := fmt.Sprintf(`"%%s".format([%s])`, tt.formatLiteral)
            literalVal, err := parseAndEval(expr, t)
            if err != nil {
                t.Fatalf("program.Eval failed: %v", err)
            }
            out, err := parseAndEval(literalVal.Value().(string), t)
            if err != nil {
                t.Fatalf("literal evaluation failed: %v", err)
            }
            if out.Type().TypeName() != tt.expectedType {
                t.Errorf("expected literal to evaluate to type %s, got %s", tt.expectedType, out.Type().TypeName())
            }
            equivalentVal, err := parseAndEval(literalVal.Value().(string)+" == "+tt.formatLiteral, t)
            if err != nil {
                t.Fatalf("equality evaluation failed: %v:", err)
            }
            if equivalentVal.Type().TypeName() != "bool" {
                t.Errorf("expected equality expression to evaluation to type bool, got %s", equivalentVal.Type().TypeName())
            }
            equivalent := equivalentVal.Value().(bool)
            if !equivalent {
                t.Errorf("%q (observed) and %q (expected) not considered equivalent", literalVal.Value().(string), tt.formatLiteral)
            }
        })
    }
}

func mustParseDuration(s string) time.Duration {
    d, err := time.ParseDuration(s)
    if err != nil {
        panic(err)
    }
    return d
}

func unquote(s string) (string, error) {
    r := []rune(sanitize(s))
    if r[0] != '"' || r[len(r)-1] != '"' {
        return "", fmt.Errorf("expected given string to be enclosed in double quotes: %q", r)
    }
    var unquotedStrBuilder strings.Builder
    noQuotes := r[1 : len(r)-1]
    for i := 0; i < len(noQuotes); {
        c := noQuotes[i]
        hasNext := i+1 < len(noQuotes)
        if c == '\\' {
            if hasNext {
                nextChar := noQuotes[i+1]
                switch nextChar {
                case 'a':
                    unquotedStrBuilder.WriteRune('\a')
                case 'b':
                    unquotedStrBuilder.WriteRune('\b')
                case 'f':
                    unquotedStrBuilder.WriteRune('\f')
                case 'n':
                    unquotedStrBuilder.WriteRune('\n')
                case 'r':
                    unquotedStrBuilder.WriteRune('\r')
                case 't':
                    unquotedStrBuilder.WriteRune('\t')
                case 'v':
                    unquotedStrBuilder.WriteRune('\v')
                case '\\':
                    unquotedStrBuilder.WriteRune('\\')
                case '"':
                    unquotedStrBuilder.WriteRune('"')
                default:
                    unquotedStrBuilder.WriteRune(c)
                    unquotedStrBuilder.WriteRune(nextChar)
                }
                i += 2
                continue
            }
        }
        unquotedStrBuilder.WriteRune(c)
        i++
    }
    return unquotedStrBuilder.String(), nil
}

func TestQuoteUnquote(t *testing.T) {
    tests := []struct {
        name                  string
        testStr               string
        expectedErr           string
        expectedOutput        string
        expectedRuntimeCost   uint64
        expectedEstimatedCost checker.CostEstimate
        disableQuote          bool
        disableCELEval        bool
    }{
        {
            name:                  "remove quotes only",
            testStr:               "this is a test",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "mid-string newline",
            testStr:               "first\nsecond",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "bell",
            testStr:               "bell\a",
            expectedEstimatedCost: checker.CostEstimate{Min: 1, Max: 1},
            expectedRuntimeCost:   1,
        },
        {
            name:                  "backspace",
            testStr:               "\bbackspace",
            expectedEstimatedCost: checker.CostEstimate{Min: 1, Max: 1},
            expectedRuntimeCost:   1,
        },
        {
            name:                  "form feed",
            testStr:               "\fform feed",
            expectedEstimatedCost: checker.CostEstimate{Min: 1, Max: 1},
            expectedRuntimeCost:   1,
        },
        {
            name:                  "carriage return",
            testStr:               "carriage \r return",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "horizontal tab",
            testStr:               "horizontal \ttab",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "vertical tab",
            testStr:               "vertical \v tab",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "double slash",
            testStr:               "double \\\\ slash",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "two escape sequences",
            testStr:               "two escape sequences \a\n",
            expectedEstimatedCost: checker.CostEstimate{Min: 3, Max: 3},
            expectedRuntimeCost:   3,
        },
        {
            name:                  "ends with slash",
            testStr:               "ends with \\",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "starts with slash",
            testStr:               "\\ starts with",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "printable unicode",
            testStr:               "printable unicode😀",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "mid-string quote",
            testStr:               "mid-string \" quote",
            expectedEstimatedCost: checker.CostEstimate{Min: 2, Max: 2},
            expectedRuntimeCost:   2,
        },
        {
            name:                  "single-quote with double quote",
            testStr:               `single-quote with "double quote"`,
            expectedEstimatedCost: checker.CostEstimate{Min: 4, Max: 4},
            expectedRuntimeCost:   4,
        },
        {
            name:                  "CEL-only escape sequences",
            testStr:               "\\? and \\`",
            expectedEstimatedCost: checker.CostEstimate{Min: 1, Max: 1},
            expectedRuntimeCost:   1,
        },
        {
            name:                  "test cost",
            testStr:               "this is a very very very long string used to ensure that cost tracking works",
            expectedEstimatedCost: checker.CostEstimate{Min: 8, Max: 8},
            expectedRuntimeCost:   8,
        },
        {
            name:         "missing opening quote",
            testStr:      `only one quote"`,
            expectedErr:  "expected given string to be enclosed in double quotes",
            disableQuote: true,
        },
        {
            name:         "missing closing quote",
            testStr:      `"only one quote`,
            expectedErr:  "expected given string to be enclosed in double quotes",
            disableQuote: true,
        },
        {
            name:           "invalid utf8",
            testStr:        "filler \x9f",
            expectedOutput: "filler " + string(utf8.RuneError),
            // disable CEL eval in order to simulate a string variable with invalid UTF-8
            disableCELEval: true,
        },
        {
            name:           "trailing single slash",
            testStr:        "\"trailing slash \\\"",
            expectedOutput: "trailing slash \\",
            disableQuote:   true,
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            var s string
            if tt.disableQuote {
                s = tt.testStr
            } else {
                if tt.disableCELEval {
                    s, _ = quote(tt.testStr)
                } else {
                    s = evalWithCEL(tt.testStr, tt.expectedRuntimeCost, tt.expectedEstimatedCost, t)
                }
            }
            output, err := unquote(s)
            if err != nil {
                if tt.expectedErr != "" {
                    if !strings.Contains(err.Error(), tt.expectedErr) {
                        t.Fatalf("expected error message %q to contain %q", err, tt.expectedErr)
                    }
                } else {
                    t.Fatalf("unexpected error: %s", err)
                }
            } else {
                if tt.expectedErr != "" {
                    t.Fatalf("expected error message with substring %q but no error was seen", tt.expectedErr)
                }
                if tt.expectedOutput != "" {
                    if output != tt.expectedOutput {
                        t.Fatalf("expected output: %q, got: %q", tt.expectedOutput, output)
                    }
                } else if output != tt.testStr {
                    t.Fatalf("input-output mismatch: original: %q, quote/unquote: %q", tt.testStr, output)
                }
            }
        })
    }
}

type noopCostEstimator struct{}

func (e *noopCostEstimator) CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64 {
    return nil
}

func (e *noopCostEstimator) EstimateCallCost(function, overloadID string, target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
    return nil
}

func (e *noopCostEstimator) EstimateSize(element checker.AstNode) *checker.SizeEstimate {
    return nil
}

func evalWithCEL(input string, expectedRuntimeCost uint64, expectedEstimatedCost checker.CostEstimate, t *testing.T) string {
    env, err := cel.NewEnv(Strings())
    if err != nil {
        t.Fatalf("cel.NewEnv() failed: %v", err)
    }
    expr := fmt.Sprintf(`strings.quote(%q)`, input)
    parsedAst, issues := env.Parse(expr)
    if issues.Err() != nil {
        t.Fatalf("env.Parse() failed: %v", issues.Err())
    }
    checkedAst, issues := env.Check(parsedAst)
    if issues.Err() != nil {
        t.Fatalf("env.Check() failed: %v", issues.Err())
    }

    costTracker := &noopCostEstimator{}
    actualEstimatedCost, err := env.EstimateCost(checkedAst, costTracker)
    if err != nil {
        t.Fatal(err)
    }
    if expectedEstimatedCost.Min != 0 && expectedEstimatedCost.Max != 0 {
        if actualEstimatedCost.Min != expectedEstimatedCost.Min && actualEstimatedCost.Max != expectedEstimatedCost.Max {
            t.Fatalf("expected estimated cost range to be %v, was %v", expectedEstimatedCost, actualEstimatedCost)
        }
    }

    program, err := env.Program(checkedAst, cel.CostTracking(costTracker))
    if err != nil {
        t.Fatal(err)
    }
    out, evalDetails, err := program.Eval(cel.NoVars())
    if err != nil {
        t.Fatal(err)
    }
    if evalDetails == nil {
        t.Fatal("evalDetails could not be calculated")
    } else if evalDetails.ActualCost() == nil {
        t.Fatal("could not calculate runtime cost")
    }
    if expectedRuntimeCost != 0 {
        if *evalDetails.ActualCost() != expectedRuntimeCost {
            t.Fatalf("expected runtime cost of %d, got %d", expectedRuntimeCost, *evalDetails.ActualCost())
        }
        if expectedEstimatedCost.Min != 0 && expectedEstimatedCost.Max != 0 {
            if *evalDetails.ActualCost() < expectedEstimatedCost.Min || *evalDetails.ActualCost() > expectedEstimatedCost.Max {
                t.Fatalf("runtime cost %d outside of expected estimated cost range %v", *evalDetails.ActualCost(), expectedEstimatedCost)
            }
        }
    }
    if out.Type() != types.StringType {
        t.Fatalf("expected expr output to be a string, got %s", out.Type().TypeName())
    }
    return out.Value().(string)
}

func FuzzQuote(f *testing.F) {
    tests := []string{
        "this is a test",
        `only one quote"`,
        `"only one quote`,
        "first\nsecond",
        "bell\a",
        "\bbackspace",
        "\fform feed",
        "carriage \r return",
        "horizontal \ttab",
        "vertical \v tab",
        "double \\\\ slash",
        "two escape sequences \a\n",
        "ends with \\",
        "\\ starts with",
        "printable unicode😀",
        "mid-string \" quote",
        "\\? and \\`",
        "filler \x9f",
        "size('ÿ')",
        "size('πέντε')",
        "завтра",
        "\U0001F431\U0001F600\U0001F61B",
        "ta©o©αT",
    }
    for _, tc := range tests {
        f.Add(tc)
    }
    f.Fuzz(func(t *testing.T, s string) {
        quoted, err := quote(s)
        if err != nil {
            if utf8.ValidString(s) {
                t.Errorf("unexpected error: %s", err)
            }
        } else {
            unquoted, err := unquote(quoted)
            if err != nil {
                t.Errorf("unexpected error: %s", err)
            } else if s != sanitize(s) {
                if unquoted != sanitize(s) {
                    t.Errorf("input-output mismatch on test case containing invalid UTF-8: sanitized original: %q, quoted: %q, quote/unquote: %q", sanitize(s), quoted, unquoted)
                }
            } else if unquoted != s {
                t.Errorf("input-output mismatch: original: %q, quoted: %q, quote/unquote: %q", s, quoted, unquoted)
            }
        }
    })
}
