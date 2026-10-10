// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"testing"
)

func TestComparisonEditSurvivesLocalRename(t *testing.T) {
	e := edit{Comparison: &comparisonEdit{Function: "apply", Left: "Revision", Right: "Highest", Before: ">", After: "!="}}
	for _, source := range []string{
		"package p; func apply() { if t.Revision > id.Highest {} }",
		"package p\nfunc apply() {\n if incoming.Revision /* authority */ > retained.Highest {\n }\n}",
	} {
		changed, err := editSource([]byte(source), e)
		if err != nil || !bytes.Equal(changed, bytes.Replace([]byte(source), []byte(">"), []byte("!="), 1)) {
			t.Fatalf("comparison did not change exactly its operator: %s (%v)", changed, err)
		}
	}
	for _, source := range []string{
		"package p; func other() { if t.Revision > id.Highest {} }",
		"package p; func apply() { if t.Revision >= id.Highest {} }",
		"package p; func apply() { if t.Revision > id.Highest {}; if a.Revision > b.Highest {} }",
	} {
		if _, err := editSource([]byte(source), e); err == nil {
			t.Fatalf("missing or ambiguous comparison accepted: %s", source)
		}
	}
}

func TestTokenEditSurvivesFormatting(t *testing.T) {
	e := edit{Before: "if !exists || e.Version > old.Version {", After: "if true {"}
	source := []byte("package p\nfunc apply() {\n if !exists || // current version\n e.Version > old.Version { keep() }\n}")
	changed, err := editSource(source, e)
	if err != nil || !bytes.Contains(changed, []byte("if true { keep() }")) {
		t.Fatalf("formatting blocked token substitution: %s (%v)", changed, err)
	}
	// A comment containing the old source is not another executable anchor.
	source = append([]byte("// "+e.Before+"\n"), source...)
	if _, err = editSource(source, e); err != nil {
		t.Fatal(err)
	}
	if _, err = editSource(append(source, source...), e); err == nil {
		t.Fatal("ambiguous token substitution accepted")
	}
	if _, err = editSource(source, edit{Before: e.Before, After: "if !exists ||\n e.Version > old.Version {"}); err == nil {
		t.Fatal("formatting-only substitution counted as a mutation")
	}
}
