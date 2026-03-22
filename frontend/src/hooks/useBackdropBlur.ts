import { useState, useEffect } from "react";

// Optional env override: VITE_BACKDROP_BLUR=true|false
// Useful when auto-detection isn't available (Firefox, Safari) or for testing.
const ENV_OVERRIDE = import.meta.env.VITE_BACKDROP_BLUR as string | undefined;

/**
 * Detects whether backdrop-filter: blur() will composite correctly on this
 * machine. Returns true on Windows 11+ and all non-Windows platforms.
 * Returns false on Windows 10 (where the Chromium compositor can produce
 * a "liquid glass" artifact when blur overlaps many canvas layers).
 *
 * Uses UA Client Hints (Chromium only). Falls back to false if unavailable.
 * The VITE_BACKDROP_BLUR env var overrides everything.
 */
async function detectBackdropBlurSupport(): Promise<boolean> {
  if (ENV_OVERRIDE !== undefined) {
    return ENV_OVERRIDE === "true";
  }

  // navigator.userAgentData is Chromium-only. Firefox/Safari fall through.
  const uaData = (navigator as Navigator & { userAgentData?: UADataValues }).userAgentData;
  if (uaData?.getHighEntropyValues) {
    try {
      const { platform, platformVersion } = await uaData.getHighEntropyValues([
        "platform",
        "platformVersion",
      ]);
      if (platform === "Windows") {
        // Windows 11 identifies as platformVersion >= 13.0.0
        // Windows 10 is < 13.0.0
        const major = parseInt((platformVersion as string).split(".")[0], 10);
        return major >= 13;
      }
      // macOS / Linux / ChromeOS — compositor is fine
      return true;
    } catch {
      // Detection failed, be conservative
      return false;
    }
  }

  // No UA Client Hints (Firefox, Safari): can't tell, assume fine
  return true;
}

// Module-level cache — detection runs at most once per page load.
let cachedResult: boolean | null = null;
const detectionPromise = detectBackdropBlurSupport().then((v) => {
  cachedResult = v;
  return v;
});

/** Returns true once it's determined that backdrop-filter is safe to use. */
export function useBackdropBlur(): boolean {
  const [enabled, setEnabled] = useState<boolean>(cachedResult ?? false);

  useEffect(() => {
    // If we already have a result (e.g. component mounts after detection
    // completed), update immediately without waiting for the promise.
    if (cachedResult !== null) {
      setEnabled(cachedResult);
      return;
    }
    let cancelled = false;
    detectionPromise.then((v) => {
      if (!cancelled) setEnabled(v);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return enabled;
}

// Minimal type stub for the non-standard userAgentData API.
interface UADataValues {
  getHighEntropyValues: (hints: string[]) => Promise<Record<string, string>>;
}
