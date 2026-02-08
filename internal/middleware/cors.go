package middleware

import (
	"net/http"
	"strings"
)

// CORS sets Access-Control-* headers. Use trusted origins in production.
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	origins := make(map[string]bool)
	for _, o := range allowedOrigins {
		s := strings.TrimSpace(o)
		if s != "" {
			origins[s] = true
		}
	}
	// Dev fallback: always allow localhost / 127.0.0.1 when no origins configured
	allowLocalhost := len(origins) == 0
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			allowed := origins[origin] ||
				(allowLocalhost && origin != "" &&
					(strings.HasPrefix(origin, "http://localhost:") || strings.HasPrefix(origin, "http://127.0.0.1:")))
			if allowed && origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Correlation-ID")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

