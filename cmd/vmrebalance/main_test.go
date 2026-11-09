package main

import "testing"

func TestVersionStringPrefersTheReleaseVersion(t *testing.T) {
	old := version
	defer func() { version = old }()

	version = "1.2.3"
	if got := versionString(); got != "1.2.3" {
		t.Errorf("with a release version: %q", got)
	}
	version = ""
	if got := versionString(); got == "" {
		t.Error("without a release version: empty, want the module version or dev")
	}
}
