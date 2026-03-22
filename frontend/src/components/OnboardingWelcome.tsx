import { useEffect } from "react";
import { createPortal } from "react-dom";

interface Props {
  onStartTour: () => void;
  onSkip: () => void;
}

export function OnboardingWelcome({ onStartTour, onSkip }: Props) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onSkip();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onSkip]);

  return createPortal(
    <div
      className="fixed inset-0 z-[9990] flex items-center justify-center bg-black/70 backdrop-blur-sm animate-fade-in"
      role="dialog"
      aria-modal="true"
      aria-labelledby="onboarding-title"
    >
      <div className="animate-fade-in-scale bg-forge-surface border border-forge-border rounded-xl shadow-2xl w-full max-w-[380px] mx-4 overflow-hidden">
        <div className="h-px bg-gradient-to-r from-transparent via-[#6366f1] to-transparent" />

        <div className="p-8">
          {/* Logo */}
          <div className="flex items-center gap-2 mb-7">
            <img src="/forge-logo.png" alt="" aria-hidden="true" className="w-6 h-6" />
            <span className="text-forge-text font-bold text-xl tracking-tight">Forge</span>
          </div>

          {/* Pipeline illustration */}
          <PipelineIllustration />

          {/* Copy */}
          <div className="mt-6">
            <h1 id="onboarding-title" className="text-forge-text text-lg font-semibold leading-snug">
              Visual pipelines,<br />faster iteration
            </h1>
            <p className="text-forge-muted text-sm mt-2 leading-relaxed">
              Connect data blocks on a canvas. Change a parameter and only the
              downstream steps re-run - not the whole pipeline.
            </p>
          </div>

          {/* CTAs */}
          <div className="mt-7 flex flex-col gap-2">
            <button
              onClick={onStartTour}
              className="w-full px-4 py-2.5 rounded-lg bg-[#6366f1] hover:bg-[#818cf8] text-white text-sm font-semibold transition-[background-color,transform] duration-150 active:scale-[0.97] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#6366f1] focus-visible:ring-offset-2 focus-visible:ring-offset-forge-surface"
            >
              Take a quick tour with toy data →
            </button>
            <button
              onClick={onSkip}
              className="w-full px-4 py-2 rounded-lg text-forge-muted hover:text-forge-text text-sm transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-forge-border"
            >
              Skip, I'll explore on my own
            </button>
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}

function PipelineIllustration() {
  return (
    <svg
      width="100%"
      height="52"
      viewBox="0 0 292 52"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      {/* Block 1 — IO / violet */}
      <rect x="0" y="10" width="72" height="32" rx="5" fill="#1e1233" stroke="#7c3aed" strokeWidth="1.5" />
      <text x="36" y="30" textAnchor="middle" fill="#a78bfa" fontSize="11" fontFamily="system-ui, sans-serif" fontWeight="600">⇄ Load</text>

      {/* Connector */}
      <line x1="72" y1="26" x2="108" y2="26" stroke="#3a3d4a" strokeWidth="1.5" strokeDasharray="3 2" />
      <polygon points="106,22 114,26 106,30" fill="#3a3d4a" />

      {/* Block 2 — Transform / sky */}
      <rect x="110" y="10" width="72" height="32" rx="5" fill="#0b2233" stroke="#0ea5e9" strokeWidth="1.5" />
      <text x="146" y="30" textAnchor="middle" fill="#7dd3fc" fontSize="11" fontFamily="system-ui, sans-serif" fontWeight="600">Δ Filter</text>

      {/* Connector */}
      <line x1="182" y1="26" x2="218" y2="26" stroke="#3a3d4a" strokeWidth="1.5" strokeDasharray="3 2" />
      <polygon points="216,22 224,26 216,30" fill="#3a3d4a" />

      {/* Block 3 — Viz / pink */}
      <rect x="220" y="10" width="72" height="32" rx="5" fill="#2d0b1a" stroke="#ec4899" strokeWidth="1.5" />
      <text x="256" y="30" textAnchor="middle" fill="#f9a8d4" fontSize="11" fontFamily="system-ui, sans-serif" fontWeight="600">📈 Plot</text>
    </svg>
  );
}
