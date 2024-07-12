package svchost

import "testing"

func TestForDisplay(t *testing.T) {
    tests := []struct {
        Input string
        Want  string
    }{
        {
            "",
            "",
        },
        {
            "example.com",
            "example.com",
        },
        {
            "invalid",
            "invalid",
        },
        {
            "localhost",
            "localhost",
        },
        {
            "localhost:1211",
            "localhost:1211",
        },
        {
            "HashiCorp.com",
            "hashicorp.com",
        },
        {
            "Испытание.com",
            "испытание.com",
        },
        {
            "münchen.de", // this is a precomposed u with diaeresis
            "münchen.de", // this is a precomposed u with diaeresis
        },
        {
            "münchen.de", // this is a separate u and combining diaeresis
            "münchen.de",  // this is a precomposed u with diaeresis
        },
        {
            "example.com:443",
            "example.com",
        },
        {
            "example.com:81",
            "example.com:81",
        },
        {
            "example.com:boo",
            "example.com:boo", // invalid, but tolerated for display purposes
        },
        {
            "example.com:boo:boo",
            "example.com:boo:boo", // invalid, but tolerated for display purposes
        },
        {
            "example.com:081",
            "example.com:81",
        },
    }

    for _, test := range tests {
        t.Run(test.Input, func(t *testing.T) {
            got := ForDisplay(test.Input)
            if got != test.Want {
                t.Errorf("wrong result\ninput: %s\ngot:   %s\nwant:  %s", test.Input, got, test.Want)
            }
        })
    }
}

func TestForComparison(t *testing.T) {
    tests := []struct {
        Input string
        Want  string
        Err   string
    }{
        {
            "",
            "",
            `empty string is not a valid hostname`,
        },
        {
            "example.com",
            "example.com",
            ``,
        },
        {
            "example.com:443",
            "example.com",
            ``,
        },
        {
            "example.com:81",
            "example.com:81",
            ``,
        },
        {
            "example.com:081",
            "example.com:81",
            ``,
        },
        {
            "invalid",
            "invalid",
            ``, // the "invalid" TLD is, confusingly, a valid hostname syntactically
        },
        {
            "localhost", // supported for local testing only
            "localhost",
            ``,
        },
        {
            "localhost:1211", // supported for local testing only
            "localhost:1211",
            ``,
        },
        {
            "HashiCorp.com",
            "hashicorp.com",
            ``,
        },
        {
            "1example.com",
            "1example.com",
            ``,
        },
        {
            "Испытание.com",
            "xn--80akhbyknj4f.com",
            ``,
        },
        {
            "münchen.de", // this is a precomposed u with diaeresis
            "xn--mnchen-3ya.de",
            ``,
        },
        {
            "münchen.de", // this is a separate u and combining diaeresis
            "xn--mnchen-3ya.de",
            ``,
        },
        {
            "blah..blah",
            "",
            `hostname contains empty label (two consecutive periods)`,
        },
        {
            "example.com:boo",
            "",
            `port portion contains non-digit characters`,
        },
        {
            "example.com:80:boo",
            "",
            `port portion contains non-digit characters`,
        },
        {
            "example.com:9999999",
            "",
            `port number is greater than 65535`,
        },
        {
            "https://example.com",
            "",
            `need just a hostname and optional port number, not a full URL`,
        },
        {
            "https://example.com:80",
            "",
            `need just a hostname and optional port number, not a full URL`,
        },
        {
            "http:80", // This is weird but technically valid as a host called "http"
            "http:80",
            ``,
        },
        {
            "https:80", // This is weird but technically valid as a host called "https"
            "https:80",
            ``,
        },
    }

    for _, test := range tests {
        t.Run(test.Input, func(t *testing.T) {
            got, err := ForComparison(test.Input)
            var errStr string
            if err != nil {
                errStr = err.Error()
            }
            if errStr != test.Err {
                t.Errorf("unexpected error\ngot error:  %s\nwant error: %s", err, test.Err)
            }
            if string(got) != test.Want {
                t.Errorf("wrong result\ninput: %s\ngot:   %s\nwant:  %s", test.Input, got, test.Want)
            }
        })
    }
}

func TestHostnameForDisplay(t *testing.T) {
    tests := []struct {
        Input string
        Want  string
    }{
        {
            "example.com",
            "example.com",
        },
        {
            "example.com:81",
            "example.com:81",
        },
        {
            "xn--80akhbyknj4f.com",
            "испытание.com",
        },
        {
            "xn--80akhbyknj4f.com:8080",
            "испытание.com:8080",
        },
        {
            "xn--mnchen-3ya.de",
            "münchen.de", // this is a precomposed u with diaeresis
        },
    }

    for _, test := range tests {
        t.Run(test.Input, func(t *testing.T) {
            got := Hostname(test.Input).ForDisplay()
            if got != test.Want {
                t.Errorf("wrong result\ninput: %s\ngot:   %s\nwant:  %s", test.Input, got, test.Want)
            }
        })
    }
}
