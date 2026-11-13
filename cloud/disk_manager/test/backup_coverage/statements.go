// Statement positions aligned with cmd/cover's statement-list accounting.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"sort"
)

type Stmt struct {
	Start, End             [2]int
	Function, Kind, Tokens string
	Lines                  []int
}
type Result struct {
	File       string
	Statements []Stmt
}

func main() {
	fs := token.NewFileSet()
	var paths []string
	if err := json.NewDecoder(os.Stdin).Decode(&paths); err != nil {
		panic(err)
	}
	out := []Result{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		f, err := parser.ParseFile(fs, path, data, 0)
		if err != nil {
			panic(err)
		}
		r := Result{File: path, Statements: []Stmt{}}
		emit := func(s ast.Stmt, name string) {
			end := s.End()
			switch x := s.(type) {
			case *ast.IfStmt:
				end = x.Body.Lbrace
			case *ast.ForStmt:
				end = x.Body.Lbrace
			case *ast.RangeStmt:
				end = x.Body.Lbrace
			case *ast.SwitchStmt:
				end = x.Body.Lbrace
			case *ast.TypeSwitchStmt:
				end = x.Body.Lbrace
			case *ast.SelectStmt:
				end = x.Body.Lbrace
			case *ast.BlockStmt:
				end = x.Lbrace
			case *ast.LabeledStmt:
				panic("label needs explicit cover accounting: " + path)
			}
			ast.Inspect(s, func(n ast.Node) bool {
				if n == nil {
					return false
				}
				if x, ok := n.(*ast.FuncLit); ok && x.Body.Lbrace < end {
					end = x.Body.Lbrace
					return false
				}
				return true
			})
			a, b := fs.Position(s.Pos()), fs.Position(end)
			d := data[a.Offset:b.Offset]
			tf := token.NewFileSet().AddFile(path, -1, len(d))
			var sc scanner.Scanner
			sc.Init(tf, d, nil, 0)
			toks := ""
			lines := []int{}
			seen := map[int]bool{}
			for {
				p, t, l := sc.Scan()
				if t == token.EOF {
					break
				}
				if t == token.SEMICOLON && l == "\n" {
					continue
				}
				if l == "" {
					l = t.String()
				}
				toks += fmt.Sprintf("%d:%s;", t, l)
				line := a.Line + tf.Position(p).Line - 1
				if !seen[line] {
					lines = append(lines, line)
					seen[line] = true
				}
			}
			r.Statements = append(r.Statements, Stmt{[2]int{a.Line, a.Column}, [2]int{b.Line, b.Column}, name, fmt.Sprintf("%T", s), toks, lines})
		}
		walk := func(body *ast.BlockStmt, name string) {
			ast.Inspect(body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.BlockStmt:
					for _, s := range x.List {
						switch s.(type) {
						case *ast.CaseClause, *ast.CommClause:
							continue
						}
						emit(s, name)
					}
				case *ast.CaseClause:
					for _, s := range x.Body {
						emit(s, name)
					}
				case *ast.CommClause:
					for _, s := range x.Body {
						emit(s, name)
					}
				case *ast.IfStmt:
					if s, ok := x.Else.(*ast.IfStmt); ok {
						emit(s, name)
					}
				}
				return true
			})
		}
		for _, decl := range f.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok {
				if fd.Body == nil {
					continue
				}
				name := fd.Name.Name
				if fd.Recv != nil {
					a, b := fs.Position(fd.Recv.Pos()).Offset, fs.Position(fd.Recv.End()).Offset
					name = string(data[a:b]) + "." + name
				}
				walk(fd.Body, name)
			} else {
				ast.Inspect(decl, func(n ast.Node) bool {
					if lit, ok := n.(*ast.FuncLit); ok {
						walk(lit.Body, "<package>")
						return false
					}
					return true
				})
			}
		}
		sort.Slice(r.Statements, func(i, j int) bool {
			a, b := r.Statements[i].Start, r.Statements[j].Start
			return a[0] < b[0] || a[0] == b[0] && a[1] < b[1]
		})
		out = append(out, r)
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		panic(err)
	}
}
