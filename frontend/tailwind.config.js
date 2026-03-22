/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        forge: {
          bg: "#0f1117",
          surface: "#1a1d27",
          border: "#2a2d3a",
          accent: "#6366f1",
          "accent-hover": "#818cf8",
          text: "#e2e8f0",
          muted: "#64748b",
          stale: "#eab308",
          running: "#3b82f6",
          complete: "#22c55e",
          error: "#ef4444",
        },
      },
    },
  },
  plugins: [],
};
