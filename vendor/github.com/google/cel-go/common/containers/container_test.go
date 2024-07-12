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

package containers

import (
    "reflect"
    "testing"

    "github.com/google/cel-go/common/ast"
)

func TestContainers_ResolveCandidateNames(t *testing.T) {
    c, err := NewContainer(Name("a.b.c.M.N"))
    if err != nil {
        t.Fatal(err)
    }
    names := c.ResolveCandidateNames("R.s")
    want := []string{
        "a.b.c.M.N.R.s",
        "a.b.c.M.R.s",
        "a.b.c.R.s",
        "a.b.R.s",
        "a.R.s",
        "R.s",
    }
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
}

func TestContainers_ResolveCandidateNames_FullyQualifiedName(t *testing.T) {
    c, err := NewContainer(Name("a.b.c.M.N"))
    if err != nil {
        t.Fatal(err)
    }
    // The leading '.' indicates the name is already fully-qualified.
    names := c.ResolveCandidateNames(".R.s")
    want := []string{"R.s"}
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
}

func TestContainers_ResolveCandidateNames_EmptyContainer(t *testing.T) {
    names := DefaultContainer.ResolveCandidateNames("R.s")
    want := []string{"R.s"}
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
}

func TestContainers_Abbrevs(t *testing.T) {
    abbr, err := DefaultContainer.Extend(Abbrevs("my.alias.R"))
    if err != nil {
        t.Fatal(err)
    }
    names := abbr.ResolveCandidateNames("R")
    want := []string{
        "my.alias.R",
    }
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
    c, err := NewContainer(Name("a.b.c"), Abbrevs("my.alias.R"))
    if err != nil {
        t.Fatal(err)
    }
    names = c.ResolveCandidateNames("R")
    want = []string{
        "my.alias.R",
    }
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
    names = c.ResolveCandidateNames("R.S.T")
    want = []string{
        "my.alias.R.S.T",
    }
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
    names = c.ResolveCandidateNames("S")
    want = []string{
        "a.b.c.S",
        "a.b.S",
        "a.S",
        "S",
    }
    if !reflect.DeepEqual(names, want) {
        t.Errorf("got %v, wanted %v", names, want)
    }
}

func TestContainers_Aliasing_Errors(t *testing.T) {
    _, err := NewContainer(Abbrevs("my.alias.R", "yer.other.R"))
    wantErr := "abbreviation collides with existing reference: " +
        "name=yer.other.R, abbreviation=R, existing=my.alias.R"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Name("a.b.c.M.N"), Abbrevs("my.alias.a", "yer.other.b"))
    wantErr = "abbreviation collides with container name: name=my.alias.a, " +
        "abbreviation=a, container=a.b.c.M.N"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Abbrevs(".bad"))
    wantErr = "invalid qualified name: .bad, wanted name of the form 'qualified.name'"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Abbrevs("bad.alias."))
    wantErr = "invalid qualified name: bad.alias., wanted name of the form 'qualified.name'"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Alias("a", "b"))
    wantErr = "alias must refer to a valid qualified name: a"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Alias("my.alias", "b.c"))
    wantErr = "alias must be non-empty and simple (not qualified): alias=b.c"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Alias(".my.qual.name", "a"))
    wantErr = "qualified name must not begin with a leading '.': .my.qual.name"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }

    _, err = NewContainer(Alias(".my.qual.name", "a"))
    wantErr = "qualified name must not begin with a leading '.': .my.qual.name"
    if err == nil || err.Error() != wantErr {
        t.Errorf("got error %v, expected %s.", err, wantErr)
    }
}

func TestContainers_Extend_Alias(t *testing.T) {
    c, err := DefaultContainer.Extend(Alias("test.alias", "alias"))
    if err != nil {
        t.Fatal(err)
    }
    if c.aliasSet()["alias"] != "test.alias" {
        t.Errorf("got alias %v wanted 'test.alias'", c.aliasSet())
    }
    c, err = c.Extend(Name("with.container"))
    if err != nil {
        t.Fatal(err)
    }
    if c.Name() != "with.container" {
        t.Errorf("got container name %s, wanted 'with.container'", c.Name())
    }
    if c.aliasSet()["alias"] != "test.alias" {
        t.Errorf("got alias %v wanted 'test.alias'", c.aliasSet())
    }
}

func TestContainers_Extend_Name(t *testing.T) {
    c, err := DefaultContainer.Extend(Name(""))
    if err != nil {
        t.Fatal(err)
    }
    if c.Name() != "" {
        t.Errorf("got %v, wanted empty name", c.Name())
    }
    c, err = DefaultContainer.Extend(Name("hello.container"))
    if err != nil {
        t.Fatal(err)
    }
    if c.Name() != "hello.container" {
        t.Errorf("got container name %s, wanted 'hello.container'", c.Name())
    }
    c, err = c.Extend(Name("goodbye.container"))
    if err != nil {
        t.Fatal(err)
    }
    if c.Name() != "goodbye.container" {
        t.Errorf("got container name %s, wanted 'goodbye.container'", c.Name())
    }
    c, err = c.Extend(Name(".bad.container"))
    if err == nil {
        t.Errorf("got %v, wanted error", c.Name())
    }
}

func TestContainers_ToQualifiedName(t *testing.T) {
    fac := ast.NewExprFactory()
    ident := fac.NewIdent(1, "var")
    idName, found := ToQualifiedName(ident)
    if !found {
        t.Errorf("got not found from %v expr, wanted found", ident)
    }
    if idName != "var" {
        t.Errorf("got %v, wanted 'var'", idName)
    }
    sel := fac.NewSelect(2, ident, "qualifier")
    qualName, found := ToQualifiedName(sel)
    if !found {
        t.Errorf("got not found from %v expr, wanted found", sel)
    }
    if qualName != "var.qualifier" {
        t.Errorf("got %v, wanted 'var.qualifier'", qualName)
    }

    pres := fac.NewPresenceTest(2, ident, "qualifier")
    _, found = ToQualifiedName(pres)
    if found {
        t.Error("got found, wanted not found for test-only expression")
    }

    unary := fac.NewCall(2, "!_", ident)
    sel = fac.NewSelect(3, unary, "qualifier")
    _, found = ToQualifiedName(sel)
    if found {
        t.Errorf("got found, wanted not found for %v", sel)
    }
}
