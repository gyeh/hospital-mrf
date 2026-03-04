package main

import (
	"strings"
	"testing"
)

func TestParseCensusResponse(t *testing.T) {
	// Sample Census batch geocoder response.
	resp := `"1","5995 Spring St, Warm Springs, GA 31830","Match","Exact","5995 Spring St, Warm Springs, GA, 31830","-84.67,32.89","123456","L"
"2","123 Fake St, Nowhere, ZZ 00000","No_Match"
"3","1100 Mercer Ave, Decatur, IN 46733","Match","Non_Exact","1100 Mercer Ave, Decatur, IN, 46733","-84.93,40.83","789012","R"
`

	results, err := parseCensusResponse(strings.NewReader(resp))
	if err != nil {
		t.Fatalf("parseCensusResponse: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First: matched exact
	if !results[0].matched {
		t.Error("result 0: expected matched=true")
	}
	if results[0].matchType != "Exact" {
		t.Errorf("result 0: matchType=%q, want Exact", results[0].matchType)
	}
	if results[0].lon != -84.67 || results[0].lat != 32.89 {
		t.Errorf("result 0: coords=(%v,%v), want (-84.67,32.89)", results[0].lon, results[0].lat)
	}

	// Second: no match
	if results[1].matched {
		t.Error("result 1: expected matched=false")
	}

	// Third: matched non-exact
	if !results[2].matched {
		t.Error("result 2: expected matched=true")
	}
	if results[2].matchType != "Non_Exact" {
		t.Errorf("result 2: matchType=%q, want Non_Exact", results[2].matchType)
	}
}
