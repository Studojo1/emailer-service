package main

import (
	"io/fs"
	"regexp"
	"testing"
)

// The admin API no longer reads ?token= (audit AS-N02), so any dashboard
// code that builds a URL with the token in it is both broken and a leak.
func TestDashboardNeverPutsTokenInURL(t *testing.T) {
	tokenInURL := regexp.MustCompile(`[?&]token=['"]?\s*\+`)
	err := fs.WalkDir(dashboardFS, "dashboard", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := dashboardFS.ReadFile(path)
		if err != nil {
			return err
		}
		if loc := tokenInURL.FindIndex(b); loc != nil {
			t.Errorf("%s: builds a URL carrying the admin token near %q", path, b[loc[0]:min(len(b), loc[1]+40)])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
