import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The built assets are embedded into the Go binary (Stage 9) and served from
// the same origin as the API, so a relative base keeps asset URLs correct.
// In `npm run dev`, /api is proxied to the Go server on :8080.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: { outDir: "dist", emptyOutDir: true },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://localhost:8080", changeOrigin: true },
    },
  },
  preview: {
    port: 4173,
    proxy: {
      "/api": { target: "http://localhost:8080", changeOrigin: true },
    },
  },
});
