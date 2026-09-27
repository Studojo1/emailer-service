package handlers

import "testing"

// Opening the outreach welcome must not count as engagement on its own: the
// gate row and every chase step of that flow need an uploaded resume too.
// The Career Coach flow is unchanged.
func TestGateNeedsUpload(t *testing.T) {
	cases := map[string]bool{
		"cc_gate_outreach_notused": true,
		"cc_outreach_nudge_d1":     true,
		"cc_outreach_nudge_d4":     true,
		"cc_gate_coach_notstarted": false,
		"cc_nudge_1":               false,
		"cc_outreach_push1":        false,
	}
	for emailType, want := range cases {
		if got := GateNeedsUpload(emailType); got != want {
			t.Errorf("GateNeedsUpload(%q) = %v, want %v", emailType, got, want)
		}
	}
}
