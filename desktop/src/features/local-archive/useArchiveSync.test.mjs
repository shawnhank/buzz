/**
 * Mounted-hook tests for the archive sync start gate.
 *
 * The gate is the only part of archive sync still living in the renderer, and
 * it is load-bearing for a reason no unit test of either hook alone can show:
 * kind 24200 is relay-*ephemeral*. Frames that arrive before the listener
 * opens are gone permanently, so `start_archive_sync` must not be invoked
 * until observer reconciliation has seeded kind 24200 into the saved
 * subscription. Everything below the gate now runs in Rust and is covered by
 * `archive/sync_tests.rs`.
 *
 * These mount the two real hooks composed exactly as AppShell composes them
 * (`useArchiveSync(useObserverArchiveReconciliation(pubkey))`) and assert on
 * the Tauri commands that actually cross the boundary. They replace the
 * ordering tests that drove the deleted `ArchiveSyncManager`: the invariant
 * survived the move to Rust, only its observable did.
 */

import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import { JSDOM } from "jsdom";
import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";

import { useObserverArchiveReconciliation } from "./useObserverArchiveSeed.ts";
import { useArchiveSync } from "./useArchiveSync.ts";

const PUBKEY = "pk1";

// ── Harness ──────────────────────────────────────────────────────────────────

/**
 * Mounts the AppShell composition and returns the invoked Tauri command names
 * in call order, plus handles to drive the gate and unmount.
 *
 * `@tauri-apps/api/core` reads `window.__TAURI_INTERNALS__.invoke` at call
 * time, so intercepting it here captures every command the hooks issue.
 */
function mountGate({ mergeShouldFail = false } = {}) {
  const dom = new JSDOM(
    "<!doctype html><html><body><div id='root'></div></body></html>",
  );
  const invoked = [];
  dom.window.__TAURI_INTERNALS__ = {
    invoke: (cmd) => {
      invoked.push(cmd);
      return Promise.resolve(null);
    },
    transformCallback: () => Math.random(),
  };
  Object.assign(globalThis, {
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
    window: dom.window,
  });

  let resolveMerge;
  let rejectMerge;
  const mergePromise = new Promise((resolve, reject) => {
    resolveMerge = resolve;
    rejectMerge = () => reject(new Error("merge failed"));
  });

  // Real reconciler, controllable merge: this is the production seam the
  // reconciliation hook already exposes for tests. Frozen so the hook's
  // effect does not re-run on every render.
  const deps = Object.freeze({
    mergeSaveSubscriptionKinds: () => mergePromise,
    readExplicitChoice: () => "unset",
    setExplicitChoice: () => {},
  });

  function Harness() {
    const reconciled = useObserverArchiveReconciliation(PUBKEY, deps);
    useArchiveSync(reconciled);
    return null;
  }

  const root = createRoot(dom.window.document.getElementById("root"));

  return {
    invoked,
    async mount() {
      await act(async () => {
        root.render(React.createElement(Harness));
      });
    },
    async settleGate() {
      await act(async () => {
        if (mergeShouldFail) rejectMerge();
        else resolveMerge();
        await mergePromise.catch(() => {});
      });
    },
    async unmount() {
      await act(async () => {
        root.unmount();
      });
    },
  };
}

afterEach(() => {
  delete globalThis.document;
  delete globalThis.window;
  delete globalThis.HTMLElement;
  delete globalThis.IS_REACT_ACT_ENVIRONMENT;
});

// ── The gate ─────────────────────────────────────────────────────────────────

describe("archive sync start gate", () => {
  it("does not start the backend task before reconciliation resolves", async () => {
    const gate = mountGate();
    await gate.mount();

    assert.deepEqual(
      gate.invoked.filter((cmd) => cmd === "start_archive_sync"),
      [],
      "start_archive_sync must not run before reconciliation resolves — " +
        "kind 24200 frames arriving before the listener opens are lost",
    );

    await gate.settleGate();

    assert.deepEqual(
      gate.invoked.filter((cmd) => cmd === "start_archive_sync"),
      ["start_archive_sync"],
      "start_archive_sync must run exactly once after the gate opens",
    );
  });

  it("never starts the backend task when reconciliation fails", async () => {
    const gate = mountGate({ mergeShouldFail: true });
    await gate.mount();
    await gate.settleGate();

    assert.deepEqual(
      gate.invoked.filter((cmd) => cmd === "start_archive_sync"),
      [],
      "a failed reconciliation leaves the gate closed",
    );
  });

  it("stops the backend task on unmount", async () => {
    const gate = mountGate();
    await gate.mount();
    await gate.settleGate();
    await gate.unmount();

    assert.deepEqual(
      gate.invoked.filter((cmd) => cmd.endsWith("_archive_sync")),
      ["start_archive_sync", "stop_archive_sync"],
      "unmount must stop the task it started, in that order",
    );
  });
});
