package main

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// web/dist is the built dashboard (Stage 8). It is committed to the repo so
// this embed always resolves and `go run .` serves the UI with no separate
// frontend server. `make build` regenerates web/dist from source before
// building the binary.
//
//go:embed all:web/dist
var distFS embed.FS

// dashboardHandler serves the embedded SPA. Paths that are not real assets
// fall back to index.html so the single-page app works under any URL.
func dashboardHandler() (http.Handler, error) {
	sub, err := fs.Sub(distFS, "web/dist")
	if err != nil {
		return nil, err
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, statErr := fs.Stat(sub, p); statErr != nil {
			r.URL.Path = "/"
		}
		files.ServeHTTP(w, r)
	}), nil
}
