package tfaddr

import (
    "fmt"
    "log"
    "testing"

    "github.com/google/go-cmp/cmp"
    svchost "github.com/hashicorp/terraform-svchost"
)

func TestParseModuleSource_simple(t *testing.T) {
    tests := map[string]struct {
        input   string
        want    Module
        wantErr string
    }{
        "main registry implied": {
            input: "hashicorp/subnets/cidr",
            want: Module{
                Package: ModulePackage{
                    Host:         svchost.Hostname("registry.terraform.io"),
                    Namespace:    "hashicorp",
                    Name:         "subnets",
                    TargetSystem: "cidr",
                },
                Subdir: "",
            },
        },
        "main registry implied, subdir": {
            input: "hashicorp/subnets/cidr//examples/foo",
            want: Module{
                Package: ModulePackage{
                    Host:         svchost.Hostname("registry.terraform.io"),
                    Namespace:    "hashicorp",
                    Name:         "subnets",
                    TargetSystem: "cidr",
                },
                Subdir: "examples/foo",
            },
        },
        "custom registry": {
            input: "example.com/awesomecorp/network/happycloud",
            want: Module{
                Package: ModulePackage{
                    Host:         svchost.Hostname("example.com"),
                    Namespace:    "awesomecorp",
                    Name:         "network",
                    TargetSystem: "happycloud",
                },
                Subdir: "",
            },
        },
        "custom registry, subdir": {
            input: "example.com/awesomecorp/network/happycloud//examples/foo",
            want: Module{
                Package: ModulePackage{
                    Host:         svchost.Hostname("example.com"),
                    Namespace:    "awesomecorp",
                    Name:         "network",
                    TargetSystem: "happycloud",
                },
                Subdir: "examples/foo",
            },
        },
    }

    for name, test := range tests {
        t.Run(name, func(t *testing.T) {
            addr, err := ParseModuleSource(test.input)

            if test.wantErr != "" {
                switch {
                case err == nil:
                    t.Errorf("unexpected success\nwant error: %s", test.wantErr)
                case err.Error() != test.wantErr:
                    t.Errorf("wrong error messages\ngot:  %s\nwant: %s", err.Error(), test.wantErr)
                }
                return
            }

            if err != nil {
                t.Fatalf("unexpected error: %s", err.Error())
            }

            if diff := cmp.Diff(addr, test.want); diff != "" {
                t.Errorf("wrong result\n%s", diff)
            }
        })
    }

}

func TestParseModuleSource(t *testing.T) {
    tests := map[string]struct {
        input           string
        wantString      string
        wantForDisplay  string
        wantForProtocol string
        wantErr         string
    }{
        "public registry": {
            input:           `hashicorp/consul/aws`,
            wantString:      `registry.terraform.io/hashicorp/consul/aws`,
            wantForDisplay:  `hashicorp/consul/aws`,
            wantForProtocol: `hashicorp/consul/aws`,
        },
        "public registry with subdir": {
            input:           `hashicorp/consul/aws//foo`,
            wantString:      `registry.terraform.io/hashicorp/consul/aws//foo`,
            wantForDisplay:  `hashicorp/consul/aws//foo`,
            wantForProtocol: `hashicorp/consul/aws`,
        },
        "public registry using explicit hostname": {
            input:           `registry.terraform.io/hashicorp/consul/aws`,
            wantString:      `registry.terraform.io/hashicorp/consul/aws`,
            wantForDisplay:  `hashicorp/consul/aws`,
            wantForProtocol: `hashicorp/consul/aws`,
        },
        "public registry with mixed case names": {
            input:           `HashiCorp/Consul/aws`,
            wantString:      `registry.terraform.io/HashiCorp/Consul/aws`,
            wantForDisplay:  `HashiCorp/Consul/aws`,
            wantForProtocol: `HashiCorp/Consul/aws`,
        },
        "private registry with non-standard port": {
            input:           `Example.com:1234/HashiCorp/Consul/aws`,
            wantString:      `example.com:1234/HashiCorp/Consul/aws`,
            wantForDisplay:  `example.com:1234/HashiCorp/Consul/aws`,
            wantForProtocol: `HashiCorp/Consul/aws`,
        },
        "private registry with IDN hostname": {
            input:           `Испытание.com/HashiCorp/Consul/aws`,
            wantString:      `испытание.com/HashiCorp/Consul/aws`,
            wantForDisplay:  `испытание.com/HashiCorp/Consul/aws`,
            wantForProtocol: `HashiCorp/Consul/aws`,
        },
        "private registry with IDN hostname and non-standard port": {
            input:           `Испытание.com:1234/HashiCorp/Consul/aws//Foo`,
            wantString:      `испытание.com:1234/HashiCorp/Consul/aws//Foo`,
            wantForDisplay:  `испытание.com:1234/HashiCorp/Consul/aws//Foo`,
            wantForProtocol: `HashiCorp/Consul/aws`,
        },
        "invalid hostname": {
            input:   `---.com/HashiCorp/Consul/aws`,
            wantErr: `invalid module registry hostname "---.com"; internationalized domain names must be given as direct unicode characters, not in punycode`,
        },
        "hostname with only one label": {
            // This was historically forbidden in our initial implementation,
            // so we keep it forbidden to avoid newly interpreting such
            // addresses as registry addresses rather than remote source
            // addresses.
            input:   `foo/var/baz/qux`,
            wantErr: `invalid module registry hostname: must contain at least one dot`,
        },
        "invalid target system characters": {
            input:   `foo/var/no-no-no`,
            wantErr: `invalid target system "no-no-no": must be between one and 64 ASCII letters or digits`,
        },
        "invalid target system length": {
            input:   `foo/var/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaah`,
            wantErr: `invalid target system "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaah": must be between one and 64 ASCII letters or digits`,
        },
        "invalid namespace": {
            input:   `boop!/var/baz`,
            wantErr: `invalid namespace "boop!": must be between one and 64 characters, including ASCII letters, digits, dashes, and underscores, where dashes and underscores may not be the prefix or suffix`,
        },
        "missing part with explicit hostname": {
            input:   `foo.com/var/baz`,
            wantErr: `source address must have three more components after the hostname: the namespace, the name, and the target system`,
        },
        "errant query string": {
            input:   `foo/var/baz?otherthing`,
            wantErr: `module registry addresses may not include a query string portion`,
        },
        "github.com": {
            // We don't allow using github.com like a module registry because
            // that conflicts with the historically-supported shorthand for
            // installing directly from GitHub-hosted git repositories.
            input:   `github.com/HashiCorp/Consul/aws`,
            wantErr: `can't use "github.com" as a module registry host, because it's reserved for installing directly from version control repositories`,
        },
        "bitbucket.org": {
            // We don't allow using bitbucket.org like a module registry because
            // that conflicts with the historically-supported shorthand for
            // installing directly from BitBucket-hosted git repositories.
            input:   `bitbucket.org/HashiCorp/Consul/aws`,
            wantErr: `can't use "bitbucket.org" as a module registry host, because it's reserved for installing directly from version control repositories`,
        },
        "local path from current dir": {
            // Can't use a local path when we're specifically trying to parse
            // a _registry_ source address.
            input:   `./boop`,
            wantErr: `a module registry source address must have either three or four slash-separated components`,
        },
        "local path from parent dir": {
            // Can't use a local path when we're specifically trying to parse
            // a _registry_ source address.
            input:   `../boop`,
            wantErr: `a module registry source address must have either three or four slash-separated components`,
        },
        "main registry implied, escaping subdir": {
            input:   "hashicorp/subnets/cidr//../nope",
            wantErr: `subdirectory path "../nope" leads outside of the module package`,
        },
        "relative path without the needed prefix": {
            input:   "boop/bloop",
            wantErr: "a module registry source address must have either three or four slash-separated components",
        },
    }

    for name, test := range tests {
        t.Run(name, func(t *testing.T) {
            addr, err := ParseModuleSource(test.input)

            if test.wantErr != "" {
                switch {
                case err == nil:
                    t.Errorf("unexpected success\nwant error: %s", test.wantErr)
                case err.Error() != test.wantErr:
                    t.Errorf("wrong error messages\ngot:  %s\nwant: %s", err.Error(), test.wantErr)
                }
                return
            }

            if err != nil {
                t.Fatalf("unexpected error: %s", err.Error())
            }

            if got, want := addr.String(), test.wantString; got != want {
                t.Errorf("wrong String() result\ngot:  %s\nwant: %s", got, want)
            }
            if got, want := addr.ForDisplay(), test.wantForDisplay; got != want {
                t.Errorf("wrong ForDisplay() result\ngot:  %s\nwant: %s", got, want)
            }
            if got, want := addr.Package.ForRegistryProtocol(), test.wantForProtocol; got != want {
                t.Errorf("wrong ForRegistryProtocol() result\ngot:  %s\nwant: %s", got, want)
            }
        })
    }
}

func ExampleParseModuleSource() {
    mAddr, err := ParseModuleSource("hashicorp/consul/aws//modules/consul-cluster")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%#v", mAddr)
    // Output: tfaddr.Module{Package:tfaddr.ModulePackage{Host:svchost.Hostname("registry.terraform.io"), Namespace:"hashicorp", Name:"consul", TargetSystem:"aws"}, Subdir:"modules/consul-cluster"}
}
