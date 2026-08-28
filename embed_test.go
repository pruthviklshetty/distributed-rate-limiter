package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The dashboard must be embedded (web/dist committed) so the binary is
// self-contained. This fails loudly if web/dist is missing or empty.
func TestDashboardHandler_ServesEmbeddedSPA(t *testing.T) {
	h, err := dashboardHandler()
	if err != nil {
		t.Fatalf("dashboardHandler: %v", err)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), "<div id=\"root\">") {
		t.Fatalf("/ did not serve index.html: %d", rr.Code)
	}

	// Unknown path -> SPA shell, not 404.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/no/such/route", nil))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), "<div id=\"root\">") {
		t.Fatalf("SPA fallback failed: %d", rr.Code)
	}
}
