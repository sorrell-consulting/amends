// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"slices"
)

// Only the two field-comparison operators need syntax selection. Their local
// variable names are irrelevant; the function, fields, and operator identify
// the guard. More involved transaction mutations remain explicit substitutions.
type comparisonEdit struct {
	Function string `json:"function"`
	Left     string `json:"left_field"`
	Right    string `json:"right_field"`
	Before   string `json:"before"`
	After    string `json:"after"`
}

func editSource(source []byte, e edit) ([]byte, error) {
	if e.Comparison != nil {
		if e.Before != "" || e.After != "" {
			return nil, fmt.Errorf("choose comparison or token substitution")
		}
		return editComparison(source, *e.Comparison)
	}
	// Scan Go tokens so indentation, comments, and line wrapping do not form
	// part of an anchor. Auto-inserted semicolons are omitted; explicit ones stay.
	tokens, spans := sourceTokens(source)
	before, _ := sourceTokens([]byte(e.Before))
	after, _ := sourceTokens([]byte(e.After))
	if len(before) == 0 || slices.Equal(before, after) {
		return nil, fmt.Errorf("empty or unchanged token substitution")
	}
	var matches []span
	for i := 0; i+len(before) <= len(tokens); i++ {
		if slices.Equal(tokens[i:i+len(before)], before) {
			matches = append(matches, span{spans[i].start, spans[i+len(before)-1].end})
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("token anchor matched %d times, want 1", len(matches))
	}
	return replaceSpan(source, matches[0], e.After), nil
}

type span struct{ start, end int }

func sourceTokens(source []byte) ([]string, []span) {
	files := token.NewFileSet()
	file := files.AddFile("", -1, len(source))
	var scan scanner.Scanner
	scan.Init(file, source, nil, 0)
	var tokens []string
	var spans []span
	for {
		position, kind, literal := scan.Scan()
		if kind == token.EOF {
			return tokens, spans
		}
		if kind == token.SEMICOLON && literal == "\n" {
			continue
		}
		if literal == "" {
			literal = kind.String()
		}
		start := file.Offset(position)
		tokens = append(tokens, literal)
		spans = append(spans, span{start, start + len(literal)})
	}
}

func editComparison(source []byte, edit comparisonEdit) ([]byte, error) {
	operators := []string{"==", "!=", "<", ">", "<=", ">="}
	if edit.Function == "" || edit.Left == "" || edit.Right == "" || edit.Before == edit.After || !slices.Contains(operators, edit.Before) || !slices.Contains(operators, edit.After) {
		return nil, fmt.Errorf("invalid comparison edit")
	}
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, "", source, 0)
	if err != nil {
		return nil, err
	}
	var matches []span
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != edit.Function || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			comparison, ok := node.(*ast.BinaryExpr)
			if !ok || comparison.Op.String() != edit.Before {
				return true
			}
			left, leftOK := comparison.X.(*ast.SelectorExpr)
			right, rightOK := comparison.Y.(*ast.SelectorExpr)
			if leftOK && rightOK && left.Sel.Name == edit.Left && right.Sel.Name == edit.Right {
				start := files.Position(comparison.OpPos).Offset
				matches = append(matches, span{start, start + len(edit.Before)})
			}
			return true
		})
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("comparison anchor matched %d times, want 1", len(matches))
	}
	return replaceSpan(source, matches[0], edit.After), nil
}

func replaceSpan(source []byte, at span, replacement string) []byte {
	var changed bytes.Buffer
	changed.Write(source[:at.start])
	changed.WriteString(replacement)
	changed.Write(source[at.end:])
	return changed.Bytes()
}
