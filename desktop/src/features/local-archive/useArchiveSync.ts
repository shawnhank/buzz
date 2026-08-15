import * as React from "react";

import { startArchiveSync, stopArchiveSync } from "@/shared/api/tauriArchive";

/**
 * Starts the native archive sync task once `ready` is true and stops it on
 * unmount.
 *
 * The `ready` gate is load-bearing and cannot move into the backend: kind
 * 24200 is relay-ephemeral, so frames emitted before the listener opens are
 * permanently lost. Observer reconciliation must have seeded kind 24200 into
 * the saved subscription before any listener opens, and only the renderer
 * knows when that finished. The backend task is therefore not self-starting.
 *
 * Everything below this call — subscribing, buffering, batching, archiving —
 * now runs in Rust; the renderer sees no archive traffic at all.
 */
export function useArchiveSync(ready: boolean): void {
  React.useEffect(() => {
    if (!ready) return;

    void startArchiveSync().catch((err: unknown) => {
      console.warn("[useArchiveSync] start_archive_sync failed:", err);
    });
    return () => {
      void stopArchiveSync().catch((err: unknown) => {
        console.warn("[useArchiveSync] stop_archive_sync failed:", err);
      });
    };
  }, [ready]);
}
