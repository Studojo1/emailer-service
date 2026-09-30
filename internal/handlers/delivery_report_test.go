package handlers

import (
	"encoding/json"
	"testing"
)

// Audit AR-A03 (30 Sep 2026): the handler read "deliveryStatus", which ACS
// never sends, so no delivery report could suppress an address. This is the
// data of a real Microsoft.Communication.EmailDeliveryReportReceived event.
const acsBounceEvent = `{
	"sender": "pranav@support.example.test",
	"recipient": "nobody@no-such-domain.example.test",
	"messageId": "00000000-0000-0000-0000-000000000001",
	"status": "Bounced",
	"deliveryStatusDetails": {"statusMessage": "DnsDomainDoesNotExist"},
	"deliveryAttemptTimeStamp": "2026-09-29T00:48:03.398Z"
}`

func TestParseDeliveryReportReadsACSStatusField(t *testing.T) {
	dr, ok := parseDeliveryReport(json.RawMessage(acsBounceEvent))
	if !ok {
		t.Fatal("a real ACS delivery report did not parse")
	}
	if dr.Status != "Bounced" || dr.MessageID != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("got %+v", dr)
	}
	if deliveryReportSuppressReason(dr.Status) != "hard_bounce" {
		t.Fatal("an ACS bounce must suppress the address")
	}
}

func TestParseDeliveryReportRejectsIncompleteData(t *testing.T) {
	for _, data := range []string{`{}`, `{"recipient":"a@b.test"}`, `{"status":"Bounced"}`, `not json`} {
		if _, ok := parseDeliveryReport(json.RawMessage(data)); ok {
			t.Errorf("%s parsed as a delivery report", data)
		}
	}
	if dr, ok := parseDeliveryReport(json.RawMessage(`{"recipient":"a@b.test","deliveryStatus":"Delivered"}`)); !ok || dr.Status != "Delivered" {
		t.Error("the legacy deliveryStatus field must still be read")
	}
}

func TestDeliveryReportSuppressReason(t *testing.T) {
	cases := map[string]string{
		"Bounced": "hard_bounce", "bounced": "hard_bounce",
		"Suppressed": "complaint", "FilteredSpam": "complaint", "Quarantined": "complaint",
		"Delivered": "", "Expanded": "", "Failed": "", "": "",
	}
	for status, want := range cases {
		if got := deliveryReportSuppressReason(status); got != want {
			t.Errorf("%q: got %q, want %q", status, got, want)
		}
	}
}
