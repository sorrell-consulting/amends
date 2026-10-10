package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
)

//go:embed dashboard.html
var dashboardHTML string

func renderDashboard(output io.Writer, observed inspection) error {
	functions := template.FuncMap{
		"milliseconds": func(nanoseconds int64) string {
			return fmt.Sprintf("%.3f ms", float64(nanoseconds)/1e6)
		},
		"json": func(value any) (string, error) {
			raw, err := json.MarshalIndent(value, "", "  ")
			return string(raw), err
		},
		"offset": func(value *int64) string {
			if value == nil {
				return "unknown"
			}
			return fmt.Sprint(*value)
		},
		"age": func(partition inspectedPartition) string {
			if partition.Pending == 0 {
				return "none pending"
			}
			if partition.PendingAgeSeconds == nil {
				return "unknown"
			}
			return fmt.Sprintf("%.1f s", *partition.PendingAgeSeconds)
		},
	}
	page, err := template.New("dashboard").Funcs(functions).Parse(dashboardHTML)
	if err != nil {
		return err
	}
	return page.Execute(output, observed)
}
