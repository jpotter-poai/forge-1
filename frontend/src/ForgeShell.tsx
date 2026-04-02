/**
 * ForgeShell wraps the entire app.
 * In Tauri mode: manages backend lifecycle, shows setup/workspace screens.
 * In browser mode: renders children immediately (backend managed externally).
 */

import { Fragment, type ReactNode, useCallback, useEffect, useLayoutEffect, useState } from "react";
import { useTauri } from "@/hooks/useTauri";
import { SetupScreen } from "@/components/SetupScreen";
import { WorkspaceSetup } from "@/components/WorkspaceSetup";
import { setApiBaseUrl } from "@/api/client";

interface Props {
  children: ReactNode;
}

export function ForgeShell({ children }: Props) {
  const { isTauri, stage, error, port, retry } = useTauri();
  const [needsWorkspaceSetup, setNeedsWorkspaceSetup] = useState<
    boolean | null
  >(null);
  const [appReady, setAppReady] = useState(false);
  const [appKey, setAppKey] = useState(0);

  useLayoutEffect(() => {
    let disposed = false;
    const cleanups: Array<() => void> = [];

    const setViewportHeight = (height: number) => {
      if (!Number.isFinite(height) || height <= 0) return;
      document.documentElement.style.setProperty(
        "--forge-app-height",
        `${height}px`,
      );
    };

    const syncFromDom = () => {
      setViewportHeight(window.visualViewport?.height ?? window.innerHeight);
    };

    const syncFromTauri = async () => {
      try {
        const { getCurrentWindow } = await import("@tauri-apps/api/window");
        const appWindow = getCurrentWindow();
        const [size, factor] = await Promise.all([
          appWindow.innerSize(),
          appWindow.scaleFactor(),
        ]);
        if (disposed) return;
        setViewportHeight(size.toLogical(factor).height);
      } catch {
        if (!disposed) syncFromDom();
      }
    };

    const handleResize = () => {
      if (isTauri) {
        void syncFromTauri();
      } else {
        syncFromDom();
      }
    };

    syncFromDom();
    window.addEventListener("resize", handleResize);
    cleanups.push(() => window.removeEventListener("resize", handleResize));

    const viewport = window.visualViewport;
    if (viewport) {
      viewport.addEventListener("resize", handleResize);
      cleanups.push(() => viewport.removeEventListener("resize", handleResize));
    }

    if (isTauri) {
      void (async () => {
        try {
          const { getCurrentWindow } = await import("@tauri-apps/api/window");
          const appWindow = getCurrentWindow();
          if (disposed) return;

          const unlistenResize = await appWindow.onResized(({ payload }) => {
            appWindow.scaleFactor()
              .then((factor) => {
                if (disposed) return;
                setViewportHeight(payload.toLogical(factor).height);
              })
              .catch(() => {
                if (!disposed) syncFromDom();
              });
          });
          cleanups.push(() => {
            unlistenResize();
          });

          const unlistenScale = await appWindow.onScaleChanged(({ payload }) => {
            if (disposed) return;
            setViewportHeight(payload.size.toLogical(payload.scaleFactor).height);
          });
          cleanups.push(() => {
            unlistenScale();
          });

          await syncFromTauri();
        } catch {
          if (!disposed) syncFromDom();
        }
      })();
    }

    return () => {
      disposed = true;
      cleanups.forEach((cleanup) => cleanup());
    };
  }, [isTauri]);

  // When the backend is ready in Tauri mode, update the API client
  // and check if workspace setup is needed
  useEffect(() => {
    if (!isTauri || stage !== "ready" || !port) return;

    setApiBaseUrl(port);

    (async () => {
      try {
        const { invoke } = await import("@tauri-apps/api/core");
        const complete = await invoke<boolean>("check_workspace_setup");
        if (complete) {
          setNeedsWorkspaceSetup(false);
          setAppKey((k) => k + 1);
          setAppReady(true);
        } else {
          setNeedsWorkspaceSetup(true);
        }
      } catch {
        setNeedsWorkspaceSetup(false);
        setAppReady(true);
      }
    })();
  }, [isTauri, stage, port]);

  // After workspace setup, restart backend so it picks up new .env paths
  const handleWorkspaceComplete = useCallback(async () => {
    setNeedsWorkspaceSetup(false);
    setAppReady(false);

    try {
      const { invoke } = await import("@tauri-apps/api/core");
      // Reset and re-run setup to restart the backend with new .env
      await invoke("reset_setup");
      retry();
    } catch {
      // If restart fails, just show the app anyway
      setAppReady(true);
    }
  }, [retry]);

  // In browser dev mode, skip everything
  if (!isTauri) {
    return <>{children}</>;
  }

  // Show setup screen while backend is starting
  if (stage !== "ready") {
    return <SetupScreen stage={stage} error={error} onRetry={retry} />;
  }

  // Show workspace setup on first run
  if (needsWorkspaceSetup) {
    return <WorkspaceSetup onComplete={handleWorkspaceComplete} />;
  }

  // Wait for app to be ready
  if (!appReady) {
    return <SetupScreen stage={stage} error={error} onRetry={retry} />;
  }

  return <Fragment key={appKey}>{children}</Fragment>;
}
